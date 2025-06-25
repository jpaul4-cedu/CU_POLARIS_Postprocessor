import warnings
import re
import os
from pathlib import Path
import pandas as pd
import json
import openmatrix as omx
from CU_POLARIS_Postprocessor.config import PostProcessingConfig
import shutil
import warnings
import subprocess
from concurrent import futures
import platform
import itertools
from typing import Union
from tqdm import tqdm
import tkinter as tk
from tkinter import filedialog
from enum import Enum
import math
import inspect

def get_working_directory_of_caller():
    # Get the current stack frame
    current_frame = inspect.currentframe()
    # Get the caller's frame (one level up the stack)
    caller_frame = current_frame.f_back
    # Get the working directory of the caller
    caller_working_directory = caller_frame.f_globals['__file__']
    return os.path.dirname(os.path.abspath(caller_working_directory))

def select_file():
    wd = get_working_directory_of_caller()
    root = tk.Tk()
    root.withdraw()  # Hide the root window
    file_path = filedialog.askdirectory(initialdir=wd)  # Open file selection dialog
    return Path(file_path)


class CaseVariableData:
    def __init__(self, variable_name:str, scenario_files, json_target:list, values:list, value_key:dict = None):
        self.var_name = variable_name
        self.scenario_file = scenario_files
        self.json_target = json_target
        self.values = values
        self.value_key = value_key

class BulkUpdateData:
    def __init__(self, possible_scenario_names:list, value, target:list):
        self.scenario_name = possible_scenario_names
        self.value = value
        self.target = target

def get_platform_info():
    system = platform.system()
    return system

def fxn():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        warnings.warn("deprecated", DeprecationWarning)

fxn()

def get_highest_iteration_folder(base_folder):
    pattern = re.compile(r'^(.*)_iteration_(\d+)?$')
    highest_n = -2
    iter_folder = None

    for folder_name in os.listdir(base_folder):
        match = pattern.match(folder_name)
        if match:
            try:
                n = int(match.group(2))
            except:
                n = -1
            if n > highest_n:
                highest_n = n
                iter_folder = Path(os.path.join(base_folder, folder_name))

    return iter_folder

def get_scale_factor(dir, config:PostProcessingConfig):
    if not isinstance(dir,Path):
        dir = Path(dir)
    scale, fleet = get_scale_factor_and_fleet_size(dir,config)
    return scale

def get_fleet_size(dir, config:PostProcessingConfig):
    if not isinstance(dir,Path):
        dir = Path(dir)
    scale, fleet = get_scale_factor_and_fleet_size(dir,config)
    return fleet

def get_scale_factor_and_fleet_size(dir, config:PostProcessingConfig):
    df = pd.DataFrame() 
    # Set current working directory
    for scen in config.scenario_file_names:
        if os.path.exists(Path(dir.as_posix() + "/model_files/" + scen)):
            scen_path = Path(dir.as_posix() + "/model_files/" + scen)
            break
    
    for sav in config.fleet_model_file_names:
        if os.path.exists(Path(dir.as_posix() + "/model_files/" + sav)):
            fleet_path = Path(dir.as_posix() + "/model_files/" + sav)
            break


   

    with open(scen_path, 'r') as file:
        scenario = json.load(file)
    with open(fleet_path,'r') as file:
        saev_fleet_model = json.load(file)
   
    
    # Extract the traffic scale factor
    tsf = scenario["Population synthesizer controls"]["traffic_scale_factor"]
    try:
        fleet_size = saev_fleet_model["Operator_1"]["Fleet_Base"]["Operator_1_TNC_FLEET_SIZE"]
    except KeyError:
        fleet_size = saev_fleet_model["Operator_1"]["Fleet_Base"]["TNC_FLEET_SIZE"]
    # Extract the directory name
    dir_name = os.path.split(os.path.split(dir.absolute())[0])[1]
    
    # Create a DataFrame for the current folder
    iteration_num = dir.name.split('_')[-1]
    dt_df = pd.DataFrame({'traffic scale factor': [tsf],"fleet size":[fleet_size], 'folder': [dir_name + f'_{iteration_num}']})
    
    # Concatenate the new DataFrame with the main DataFrame
    df = pd.concat([df, dt_df], ignore_index=True)
    unique_values = df["traffic scale factor"].unique()
    unique_fleet = df["fleet size"].unique()
    #if len(unique_values)!=1:
     #   raise ValueError("There are mismatched traffic scale factors in your data set. Please validate.")
    traffic_scale_factor = unique_values[0]
    trip_multiplier = 1/traffic_scale_factor
    return trip_multiplier, unique_fleet[0]

def get_timeperiods(dir):
    def _process_omx(time_perios_loc): 
        with omx.open_file(time_perios_loc, 'r') as omx_file:
        # List all the matrices in the OMX file
        
            matrix_names = list(omx_file.list_matrices())

            # Initialize a list to store the timeperiod attributes
            timeperiods = []

            # Iterate over each matrix in the OMX file
            for name in matrix_names:
                # Access the matrix metadata
                
                timeperiod = int(omx_file[name].attrs.timeperiod.decode('utf-8'))
                metric = omx_file[name].attrs.metric.decode('utf-8')
                if metric == 'time':
                    timeperiods.append(timeperiod)
        return timeperiods
    
    
    try:
        time_perios_loc=Path(dir.as_posix() + '/highway_skim_file.omx')
        timeperiods = _process_omx(time_perios_loc)

    except AttributeError as e:
        time_perios_loc=Path(dir.parent.as_posix() + '/highway_skim_file.omx')
        timeperiods = _process_omx(time_perios_loc)
    return timeperiods

def get_tnc_pricing(dir, config:PostProcessingConfig):

    # Set current working directory
    
    # Paths to JSON files
    for scen in config.scenario_file_names:
        if os.path.exists(Path(dir.as_posix() + "/model_files/" + scen)):
            scen_path = Path(dir.as_posix() + "/model_files/" + scen)
            break
    
    with open(scen_path, 'r') as file:
        scenario = json.load(file)
    
    # Extract the traffic scale factor
    per_minute = scenario["Scenario controls"]["rideshare_cost_per_minute"]
    per_mile = scenario["Scenario controls"]["rideshare_cost_per_mile"]
    base = scenario["Scenario controls"]["rideshare_base_fare"]
    return per_minute,per_mile,base

def get_repositioning_interval(dir, config:PostProcessingConfig):

    # Set current working directory
    
    # Paths to JSON files
    for scen in config.fleet_model_file_names:
        if os.path.exists(Path(dir.as_posix() + "/model_files/" + scen)):
            scen_path = Path(dir.as_posix() + "/model_files/" + scen)
            break
    
    with open(scen_path, 'r') as file:
        scenario = json.load(file)
    
    #Extract the repositioning interval
    interval = scenario["DRS_Discount_AR"]["repositioning_batched_interval_seconds"]
    if interval is None:
        interval = 900

    return interval

def get_heur_discount(dir):

    # Set current working directory
    
    # Paths to JSON files
    json_path = Path(dir.parent.as_posix() + "/PoolingModel.json")
    
    try:
        with open(json_path, 'r') as file:
            scenario = json.load(file)
    except:
        print('No pooling model file in base case directory.')
        
    
    # Extract the traffic scale factor
    disc_rate = scenario["TNC_Pooling_Model"]["CU_DEFAULT_POOLING_DISCOUNT"]
   
    return disc_rate

def separate_keys_by_value(input_dict):
    categories = {
        "sql": [],
        "postprocessing": [],
        "sql_helper": [],
        "postprocessing_helper":[]
    }

    for key, value in input_dict.items():
        if value in categories:
            categories[value].append(key)
        else:
            print(f"Warning: The value '{value}' for key '{key}' is not recognized.")
    
    return categories   

def split_by_last_delimiter(value, delimiter="_"):
    """Split a string by the last occurrence of the delimiter."""
    parts = value.rsplit(delimiter, 1)
    return parts[0] if len(parts) > 1 else value

def check_value_in_list(value, lst, delimiter="_"):
    """Check if a value is in the list after splitting each element by the last occurrence of the delimiter.
       Raise an error if the value is not found.
    """
    split_values = [split_by_last_delimiter(item, delimiter) for item in lst]
    if value not in split_values:
        raise ValueError(f"Value '{value}' not found in the list after splitting by the last '{delimiter}'")
    return True

def update_scenario_file(scenario_file,full_path:str, val:Union[int,str,float], target:list = []):
    if isinstance(scenario_file,str):
        scenario_files = [scenario_file]
    elif isinstance(scenario_file,list):
        scenario_files = scenario_file
    else:
        raise TypeError("Unhandled input type.")
    for scenario_file in scenario_files:
        if os.path.exists(scenario_file):
            shutil.copy(scenario_file,os.path.join(os.path.dirname(scenario_file),os.path.splitext(os.path.basename(scenario_file))[0]+'_backup.json'))
            with open(scenario_file, 'r') as file:
                scenario = json.load(file)
            
            current = scenario
            #check if the target is in the scenario file
            for key in target[:-1]:
                if key not in current:
                    resp = input(f"Key {key} not found in {scenario_file}. Continue?[y/n]")
                    if resp.lower() == 'y':
                        current[key] = {}
                    else:
                        print("Stopping the process.")
                        quit()
                    
                current = current[key]
            current[target[-1]] = val
            #['Operator_1']['Fleet_Base']['Operator_1_TNC_MAX_WAIT_TIME']

            with open(scenario_file, 'w') as file:
                json.dump(scenario,file,indent=4)
            
        else:
            warnings.warn(f"Scenario file {scenario_file} not found in {full_path}.")

def check_scenario_file(scenario_file:str,full_path:str, target:list = []):
    if os.path.exists(scenario_file):
        with open(scenario_file, 'r') as file:
            scenario = json.load(file)
        
        current = scenario
        #check if the target is in the scenario file
        for key in target[:-1]:
            if key not in current:
                resp = input(f"Key {key} not found in {scenario_file}. Continue?[y/n]")
                if resp.lower() == 'y':
                    current[key] = {}
                else:
                    print("Stopping the process.")
                    quit()
                
            current = current[key]
           
        print(f"{os.path.basename(full_path)}, {target[-1]}, {current[target[-1]]}")
        #['Operator_1']['Fleet_Base']['Operator_1_TNC_MAX_WAIT_TIME']
    else:
        warnings.warn(f"Scenario file {scenario_file} not found in {full_path}.")

def bulk_update_scenario_files(target:list,city:str, val, folder_path:Path = os.getcwd(), scenario_names: list = ['scenario_abm.json']):
    updates = []
    for folder in os.listdir(folder_path):
        full_path = os.path.join(folder_path,folder)
        if os.path.exists(full_path) and city in folder:
            for scen_file in scenario_names:
                scenario_file = os.path.join(full_path,scen_file)
                updates.append({"scenario_file":scenario_file,"full_path":full_path,"val":val,"target":target})
    
        else:
            warnings.warn("This folder somehow does not exist despit you looking it up by name... weird.")
    for update in tqdm(updates,desc="Updating Scenario Files"):
        update_scenario_file(**update)
    
def bulk_check_scenario_files(target:list, folder_path:Path = os.getcwd(), scenario_names: list = ['scenario_abm.json']):
    for folder in os.listdir(folder_path):
        full_path = os.path.join(folder_path,folder)
        if os.path.exists(full_path):
            for scen_file in scenario_names:
                scenario_file = os.path.join(full_path,scen_file)
                check_scenario_file(scenario_file,full_path,target)
        else:
            warnings.warn("This folder somehow does not exist despit you looking it up by name... weird.")

def bulk_check_scenario_files(target:list, folder_path:Path = os.getcwd(), scenario_names: list = ['scenario_abm.json']):
    for folder in os.listdir(folder_path):
        full_path = os.path.join(folder_path,folder)
        if os.path.exists(full_path):
            for scen_file in scenario_names:
                scenario_file = os.path.join(full_path,scen_file)
                check_scenario_file(scenario_file,full_path,target)
        else:
            warnings.warn("This folder somehow does not exist despit you looking it up by name... weird.")

def call_ps_action(action):
    
    try:
        process= subprocess.Popen(action,shell=True, stdout=subprocess.DEVNULL)
        stdout, stderr = process.communicate(timeout=600)
        if stdout:
            print(stdout.decode())
        if stderr:
            print(stderr.decode())
    except subprocess.TimeoutExpired:
        print("Process timed out")
        process.kill()  # Kill the process if it times out
        stdout, stderr = process.communicate()  # Clean up
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if process.poll() is None:  # Check if process is still running
            process.kill()  # Ensure it’s terminated

def check_city_prefix(path):
    hold = os.path.basename(path)
    if len(os.path.basename(path)) == 3:
        return os.path.basename(path)
    if len(os.path.basename(path).split('_')[0]) ==3:
        return os.path.basename(path).split('_')[0]
    else:
        return None

def copy_cases(new_case_path, case_path = '', move_cases: bool = True, new_cases:list =[], keep_files: list = [], keep_suffixes:list = [], parallel: bool = False, check_city: bool = False, include_dirs: bool =False):
    
    debug = False
    skip = False
    moves ={}
    if move_cases: #just completely copy a set of cases from one place to another
        type = "copy"
        items =[]
        
        if include_dirs:
            base_path = case_path
            dir_list = []
            file_list = []
            for current_folder, sub_folders, files in os.walk(case_path):
                rel_fold = os.path.relpath(current_folder,case_path)
                if rel_fold not in dir_list and current_folder != case_path:
                    dir_list.append(rel_fold)
                    for sub_files in files:
                        rel_path = os.path.join(rel_fold,sub_files)
                        file_list.append(rel_path)
            for dir in dir_list:
                home_dir = os.path.join(case_path,dir)
                new_dir = os.path.join(new_case_path,dir)
                if not os.path.exists(new_dir):
                    os.makedirs(new_dir)
            
            for tar_file in file_list:
                home_file = os.path.join(case_path,tar_file)
                dest_file = os.path.join(new_case_path,tar_file)
                items.append((home_file,dest_file))

        else:
            folds = os.listdir(case_path)
            if not os.path.isdir(new_case_path):
                os.makedirs(new_case_path)
            for fold in folds:
                full_path = os.path.join(case_path,fold)
                if os.path.isfile(full_path):
                    pass
                elif os.path.isdir(full_path):
                    fold_suffix = fold.split('_')[-1]
                    if keep_suffixes !=None and keep_suffixes != []:
                        if fold_suffix in keep_suffixes:
                            continue
                    old_folder_name = os.path.basename(full_path)
                    new_fold = os.path.join(new_case_path,old_folder_name)
                    actions =[]
                    if not os.path.isdir(new_fold):
                        os.makedirs(new_fold)
                    for item in os.listdir(full_path):
                        sub_path = os.path.join(full_path,item)
                        if os.path.isfile(sub_path) and not os.path.isfile(os.path.join(new_fold,item)):
                            move = (sub_path,os.path.join(new_fold,item))
                            items.append(move)
                
        moves[type] = items

    elif new_cases == [] and (not os.path.isdir(new_case_path) or len(os.listdir(new_case_path))== 0):
        warnings.warn("I don't know what cases I am supposed to copy or there is nothing here for me to update.")
    elif new_cases != []: #take a single base folder and expand it into a bunch of cases
        #warnings.warn("This has not yet been debugged... proceed with caution.")
        type = "copy"
        items =[]
        
        
        case_prefix = os.path.basename(case_path)
        if "_" in case_prefix:
            case_prefix = case_prefix.split("_")[0]

        all_vars = []
        for case_var in new_cases:
            vars = case_var.values
            all_vars.append(vars)
        
        if len(new_cases)>1:
            combinations = list(itertools.product(*all_vars))
        else:
            combinations = [all_vars]
        suffixes = []
        all_vals = []
        for combination in combinations:
            suffix = '_'.join(list(map(str, combination)))
            suffixes.append(suffix)
            
        
        folds = []
        copy = True
        for suffix in suffixes:
            fold = case_prefix + '_' + suffix
            folds.append(fold)
            
            if not debug:
                if os.path.isdir(os.path.join(new_case_path,fold)):
                    pass
                else:
                    os.makedirs(os.path.join(new_case_path,fold))
            
            file_paths = []
            for root, dirs, files in os.walk(case_path):
                # Skip any directory with "iteration" in its name
                if "iteration" in root.lower() or root.startswith("."):
                    continue

                for file in files:
                    if file.startswith("."):
                        continue
                    # Full original path
                    src_path = os.path.join(root, file)
                    
                    # Relative path from the source root
                    rel_path = os.path.relpath(src_path, case_path)
                    # Destination full path
                    dest_path = os.path.join(os.path.join(new_case_path,fold), rel_path)
                    file_paths.append((src_path,dest_path))
                    # Ensure the destination directory exists
                    os.makedirs(os.path.dirname(dest_path), exist_ok=True)

            
            
            for file_path in file_paths:
                if os.path.isfile(file_path[0]):
                    if os.path.isfile(file_path[1]) and not skip:
                        if not copy:
                            continue
                        resp = input(f"File {file} already exists in {os.path.join(new_case_path,fold)}. Continue (c for continue without copying)?[y/n/c]")
                        if resp.lower() == 'y':
                            resp_all = input("Do you want to skip all future prompts?[y/n]")
                            if resp_all.lower() == 'y':
                                skip = True
                            
                        elif resp.lower() == 'c':
                            resp_all = input("Do you want to skip all future prompts?[y/n]")
                            if resp_all.lower() == 'y':
                                copy = False
                                continue
                            else:
                                continue
                        else:
                            print("Stopping the process.")
                            quit()
                                
            
                    
                    items.append(file_path)
        moves[type] = items                       

    elif keep_files != [] and case_path !='': #copy some files from a primary source folder but keep some of the case files
        type1 = "copy"
        type2 = "delete"
        items1 =[]
        items2 = []
        if os.path.isdir(new_case_path):
            all_exist = True
            for fold in os.listdir(new_case_path):
                fold_path = os.path.join(new_case_path, fold)
                if os.path.isfile(fold_path):
                    pass
                elif os.path.isdir(fold_path):
                    
                    for file_name in keep_files:
                        if file_name not in os.listdir(fold_path):
                            all_exist = False
            if not all_exist:
                raise FileNotFoundError(f"The files you requested to not update ({keep_files}) do not exist in all the folders you are attempting to update in {new_case_path}, therefore, I have stopped the process.")
            else:
                for fold in os.listdir(new_case_path):
                    fold_path = os.path.join(new_case_path, fold)
                    if os.path.isfile(fold_path):
                        pass
                    elif os.path.isdir(fold_path):
                        for file_name in os.listdir(fold_path):
                            if file_name not in keep_files:
                                file_path = os.path.join(fold_path,file_name)
                                move = (file_path)
                                items2.append(move)
                        
                        for file_name in os.listdir(case_path):
                            if file_name not in keep_files:
                                file_path = os.path.join(case_path,file_name)
                                move = (file_path,fold_path)
                                items1.append(move)
                
                moves[type1] = items1
                moves[type2] = items2
                
        else:
            raise FileNotFoundError(f"Nothing in your requested case path {new_case_path}.")
    else:
        raise NotImplementedError("This combination of arguments is not handled.")
    
    if len(moves) > 0:
        copy_tasks, del_tasks = create_tasks(moves)
        if len(copy_tasks)>0 or len(del_tasks)>0:
            run_tasks(parallel,copy_tasks,del_tasks)
    if new_cases != []:
        update_new_scenarios(new_cases, combinations, new_case_path, folds)

def update_new_scenarios(new_cases:list, combinations:list, new_case_path:str, folds:list):
    i = 0
    for case_var in new_cases:
        scen = case_var.scenario_file
        target = case_var.json_target
        if case_var.value_key:
            key = case_var.value_key
        j=0
        for vals in combinations:
            val = vals[i]
            if case_var.value_key:
                val = key[val]
            fold_path = os.path.join(new_case_path,folds[j])
            update_scenario_file([os.path.join(fold_path,_) for _ in scen] if isinstance(scen,list) and len(scen)>1 else os.path.join(fold_path,scen),fold_path,val,target)
            j+=1
        i+=1

def create_tasks(moves:dict):
    copy_tasks = []
    del_tasks = []
    platform = get_platform_info()
    for key, items in moves.items():
        if key == "copy":
            tasks = []
            for item in items:
                old_path = item[0]
                new_path = item[1]
                if platform == 'Windows':
                    action = ['copy',old_path,new_path]
                else:
                    action = ' '.join(['cp -R',f"\"{Path(old_path).as_posix()}\"",f"\"{Path(new_path).as_posix()}\""])
                copy_tasks.append(action)
        elif key == "delete":
            del_tasks = []
            for item in items:
                old_path = item
                if platform == 'Windows':
                    action = ['del',old_path]
                else:
                    action = ' '.join(['rm -r',f"\"{Path(old_path).as_posix()}\""])
                del_tasks.append(action)
    return copy_tasks, del_tasks

def run_tasks(parallel:bool = True, copy_tasks:list = [], del_tasks:list = []):
    # Set max_workers to the number of cores on the machine
    max_workers = os.cpu_count()+4
    platform = get_platform_info()
    if parallel:
        if platform == 'Windows':
            with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                list(tqdm(executor.map(call_ps_action, del_tasks),total=len(del_tasks),desc="Deleting Items"))
            with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                list(tqdm(executor.map(call_ps_action, copy_tasks),total=len(copy_tasks),desc="Moving/Copying Items")) 
        else:
            with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                list(tqdm(executor.map(execute_task, del_tasks),total=len(del_tasks),desc="Deleting Items"))
            with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                list(tqdm(executor.map(execute_task, copy_tasks),total=len(copy_tasks),desc="Moving/Copying Items"))
    else:
        if platform == 'Windows':
            for action in del_tasks:
                call_ps_action(action)
            for action in copy_tasks:
                call_ps_action(action)
        else:
            for action in del_tasks:
                execute_task(action)
            for action in copy_tasks:
                execute_task(action)

def execute_task(action):
    call_ps_action(action)

class MailType(Enum):
    NONE = "NONE"
    BEGIN = "BEGIN"
    END = "END"
    FAIL = "FAIL"
    REQUEUE = "REQUEUE"
    ALL = "ALL"

class CityKey( ):
    def __init__(self, city_name:str, city_prefix:str, city_scale_factor:float, city_mem:float, city_time=int):
        self.city_name = city_name
        self.city_prefix = city_prefix
        self.city_scale_factor = city_scale_factor
        self.city_mem = city_mem
        self.city_time = city_time

class PolarisRunConfig():
    def __init__(self, container_path:str, run_script:str, threads:int, abm_init:bool, skim_init:bool, abm_runs:int,cities: list):
        self.container_path = container_path
        if os.path.split(run_script)[0] in ["home","scratch"]:
            warnings.warn("This file is usually located inside the container, but doesn't have to be. You need to manually bind this if so.")
        self.run_script = run_script
        self.threads = threads
        self.abm_init = abm_init
        self.skim_init = skim_init
        self.abm_runs = abm_runs
        self.cities = cities
        for city in self.cities:
            city_time = city.city_time
            total_time = city_time * self.abm_runs
            frac_part, int_part = math.modf(total_time)
            hr = int(int_part)
            min = frac_part * 60
            if len(str(min)) == 1:
                min = '0'  + str(int(min))
            city.city_total_time = f"""{hr}:{int(min) if len(str(int(min)))==2 else "0"+str(int(min))}:00"""
        

def create_jobscript(job_name:str = "",nodes:int =1,cpus_per_task:int = 8,
                     mail_user:str="", mail_type: MailType = MailType.NONE,case_dir:str = "",config:PolarisRunConfig = None, redo_files = [], custom_foot = None):
    
    citykeys = config.cities

    if case_dir == "":
        print("Please provide a case directory.")
        return
    
    #create folder called run in case directory
    run_dir = os.path.join(case_dir,"run")
    if not os.path.exists(run_dir):
        os.makedirs(run_dir)
    #loop through the folders in the case directory and expand the tree until a folder level containing at least one .sqlite file is found. Retain these folder paths in a list
    folders_with_sqlite = []
    for root, dirs, files in os.walk(case_dir):
        for file in files:
            if file.endswith('.sqlite') and "iteration" not in root and "pop_syn" not in root:
                folders_with_sqlite.append(root)
                break  # No need to check other files in this folder
    if len(folders_with_sqlite) == 0:
        print("No .sqlite files found in the case directory.")
        return
    
    cities = []
    city_folder_dict = {}
    for fold in folders_with_sqlite:
        city = check_city_prefix(fold)
        cities.append(city)
        folds = city_folder_dict.get(city,[])
        folds.append(fold)
        city_folder_dict[city] = folds

    unique_cities = list(set(cities))
    city_dict = {city.city_prefix: city for city in citykeys}

    

    for city in unique_cities:
        script_path = os.path.join(run_dir,f"jobscript_{city}.sh")
        formatted_paths = "\n\t".join([f'"{path}"' for path in city_folder_dict[city] if (os.path.split(path)[-1] in redo_files and len(redo_files)>0) or len(redo_files)==0])
        if city in city_dict:
            city_info = city_dict[city]
            time = city_info.city_total_time
            mem_per_task = city_info.city_mem

        jobscript_header = \
        f"""#!/bin/bash\n#SBATCH --job-name={job_name}\n#SBATCH --nodes={nodes}\n#SBATCH --time={time}\n#SBATCH --cpus-per-task={cpus_per_task}\n#SBATCH --mem={mem_per_task}gb\n#SBATCH --mail-user={mail_user}"""
            #SBATCH --mail-type={mail_type}"""

        case_cnt = len(formatted_paths.splitlines())
        if case_cnt> 1:
            jobscript_header += f"""\n#SBATCH --array=0-{case_cnt-1}"""
            jobscript_header += f"""\n\nPATHS=(\n\t{formatted_paths}\n\t\t)"""
            jobscript_header += f"""\n\nSCENARIO_PATH=${{PATHS[$SLURM_ARRAY_TASK_ID]}}"""

        jobscript_header += """\n\n#SBATCH --output=${LOG_DIR}/output_%A_%a.out\n#SBATCH --error=${LOG_DIR}/error_%A_%a.err"""
        if not custom_foot is None:
            jobscript_header+= f"""\n\n{custom_foot}"""
        else:
            jobscript_header += f"""\n\napptainer exec --bind "$SCENARIO_PATH":/mnt/data {config.container_path} python3 {config.run_script} /mnt/data {config.threads} {config.abm_init} {config.skim_init} {config.abm_runs} {city_info.city_name} {city_info.city_scale_factor}"""

        with open(script_path, 'w') as file:
            file.write(jobscript_header)