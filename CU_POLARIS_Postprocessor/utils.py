import warnings
import re
import os
from pathlib import Path
import pandas as pd
import json
import openmatrix as omx
from CU_POLARIS_Postprocessor.config import PostProcessingConfig


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

