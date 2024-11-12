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


def fxn():
    warnings.warn("deprecated", DeprecationWarning)

with warnings.catch_warnings(action="ignore"):
    fxn()

def get_highest_iteration_folder(base_folder):
    pattern = re.compile(r'^(.*)_iteration_(\d+)$')
    highest_n = -1
    iter_folder = None

    for folder_name in os.listdir(base_folder):
        match = pattern.match(folder_name)
        if match:
            n = int(match.group(2))
            if n > highest_n:
                highest_n = n
                iter_folder = Path(os.path.join(base_folder, folder_name))

    return iter_folder

def get_scale_factor(dir, config:PostProcessingConfig):
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
    fleet_size = saev_fleet_model["Operator_1"]["Fleet_Base"]["Operator_1_TNC_FLEET_SIZE"]
    # Extract the directory name
    dir_name = os.path.split(os.path.split(dir.absolute())[0])[1]
    
    # Create a DataFrame for the current folder
    dt_df = pd.DataFrame({'traffic scale factor': [tsf],"fleet size":[fleet_size], 'folder': [dir_name + '_7']})
    
    # Concatenate the new DataFrame with the main DataFrame
    df = pd.concat([df, dt_df], ignore_index=True)
    unique_values = df["traffic scale factor"].unique()
    #if len(unique_values)!=1:
     #   raise ValueError("There are mismatched traffic scale factors in your data set. Please validate.")
    traffic_scale_factor = unique_values[0]
    trip_multiplier = 1/traffic_scale_factor
    return trip_multiplier

def get_timeperiods(dir):
    time_perios_loc=Path(dir.as_posix() + '/highway_skim_file.omx')
    try:
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
    except AttributeError as e:
            print(f"{e} attribute error in omx {dir}")
            omx_file.close()
            raise
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

def bulk_update_scenario_files(path:list, val, folder_path:Path = os.getcwd(), scenario_names: list = ['scenario_abm.json']):
    for folder in os.listdir(folder_path):
        full_path = os.path.join(folder_path,folder)
        if os.path.exists(full_path):
            for scen_file in scenario_names:
                scenario_file = os.path.join(full_path,scen_file)
                if os.path.exists(scenario_file):
                    shutil.copy(scenario_file,os.path.join(full_path,os.path.splitext(os.path.basename(scenario_file))[0]+'_backup.json'))
                    with open(scenario_file, 'r') as file:
                        scenario = json.load(file)
                    
                    current = scenario
                    for key in path[:-1]:
                        current = current[key]
                    current[path[-1]] = val
                    #['Operator_1']['Fleet_Base']['Operator_1_TNC_MAX_WAIT_TIME']

                    with open(scenario_file, 'w') as file:
                        json.dump(scenario,file,indent=4)
                    
                else:
                    warnings.warn(f"Scenario file {scen_file} not found in {full_path}.")
        else:
            warnings.warn("This folder somehow does not exist despit you looking it up by name... weird.")

def call_ps_action(action):
    try:
        process= subprocess.Popen(action,shell=True)
        stdout, stderr = process.communicate(timeout=600)
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
    if len(os.path.basename(path).split('_')[0]) ==3:
        return os.path.basename(path).split('_')[0]
    else:
        return None


def copy_cases(new_case_path, case_path = '', copy_cases: bool = True, new_cases:list = [], keep_files: list = [], keep_suffixes:list = [], parallel: bool = False, check_city: bool = False):
    tasks = []
    del_tasks = []
    if check_city and check_city_prefix(new_case_path):
        city = check_city_prefix(new_case_path)
        
        

    if copy_cases: #just completely copy a set of cases from one place to another
        folds = os.listdir(case_path)
        if not os.path.isdir(new_case_path):
            os.makedirs(new_case_path)
        for fold in folds:
            full_path = os.path.join(case_path,fold)
            if os.path.isfile(full_path):
                pass
            elif os.path.isdir(full_path):
                fold_suffix = fold.split('_')[-1]
                if fold_suffix in keep_suffixes:
                    if parallel:
                        task = [new_case_path,full_path]
                        tasks.append(task)
                    else:
                        copy_to_new_fold(new_case_path,full_path)

        if parallel:
            # Set max_workers to the number of cores on the machine
            max_workers = os.cpu_count()
            with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                executor.map(lambda task: copy_to_new_fold(*task), tasks)

    elif new_cases == [] and (not os.path.isdir(new_case_path) or len(os.listdir(new_case_path))== 0):
        warnings.warn("I don't know what cases I am supposed to copy or there is nothing here for me to update.")
    elif new_cases != []: #take a single base folder and expand it into a bunch of cases
        warnings.warn("Haven't completed this utility yet.")
    elif keep_files != [] and case_path !='': #copy some files from a primary source folder but keep some of the case files
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
                                action = ['del',file_path]
                                if parallel:
                                    del_tasks.append(action)
                                
                        for file_name in os.listdir(case_path):
                            if file_name not in keep_files:
                                file_path = os.path.join(case_path,file_name)
                                action = ['copy',file_path,fold_path]
                                if parallel:
                                    tasks.append(action)
                if parallel:
                    # Set max_workers to the number of cores on the machine
                    max_workers = os.cpu_count()
                    with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                        executor.map(lambda task: call_ps_action(*task), del_tasks)
                    with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                        executor.map(lambda task: call_ps_action(*task), tasks)  

        else:
            raise FileNotFoundError(f"Nothing in your requested case path {new_case_path}.")
    else:
        raise NotImplementedError("This combination of arguments is not handled.")

def copy_to_new_fold(new_case_path, old_folder_path):
    old_folder_name = os.path.basename(old_folder_path)
    new_fold = os.path.join(new_case_path,old_folder_name)
    if not os.path.isdir(new_fold):
        os.makedirs(new_fold)
    for item in os.listdir(old_folder_path):
        sub_path = os.path.join(old_folder_path,item)
        if os.path.isfile(sub_path) and not os.path.isfile(os.path.join(new_fold,item)):
            action = ['copy',sub_path,os.path.join(new_fold,item)]
            call_ps_action(action)
            