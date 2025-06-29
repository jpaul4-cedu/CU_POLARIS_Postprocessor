import concurrent.futures
from pathlib import Path
import os
from .utils import get_highest_iteration_folder, get_scale_factor, get_db_name
from .queries import get_sql_create
import tarfile
import re
import sqlite3
from .config import PostProcessingConfig
import pandas as pd
import itertools
import shutil
import json
from .postprocessing import process_batch_nearest_stops, process_elder_request_agg, process_nearest_stops, process_solo_equiv_fare, process_tnc_stat_summary, process_demo_financial_case_data, process_tnc_repositioning_success_rate

def parallel_process_folders(config:PostProcessingConfig):
    # Get a list of all subfolders in the parent folder
    parent_folder = config.base_dir
    folders = config.unique_folders
    

    # Use ThreadPoolExecutor or ProcessPoolExecutor to process folders in parallel
    workers = os.cpu_count()
    #workers = 1

    if config.parallel:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            results = list(executor.map(process_folder, folders, itertools.repeat(config)))
    else:
        results = [process_folder(folder, config) for folder in folders]

    collected_results={}

    for result in results:
        if not len(result) == 0:
            for key, df in result.items():
                if key not in collected_results:
                    collected_results[key]=[]
                collected_results[key].append(df)
    final_results = {key: pd.concat(df_list, ignore_index=True) for key, df_list in collected_results.items()}
    for key, df in final_results.items():
        df.to_csv(config.analysis_folder +'/' + key + '.csv', index=False)
    final_results.update(config.results)
    config.update_config(results=final_results)
    if config.output_h5:
        with pd.HDFStore(config.analysis_folder +'/results.h5') as store:
            for key, df in final_results.items():
                store[key] = df
    return True

def process_folder(dir, config:PostProcessingConfig):
    dir = get_highest_iteration_folder(dir)
    print(f"starting on directory {dir}")

    db_paths = config.folder_db_map[dir]
    supply_db = db_paths["supply"]
    demand_db = db_paths["demand"]
    result_db = db_paths['result']
    trip_multiplier = db_paths["trip_multiplier"]
    
    queries = get_sql_create(config=config,dir=dir)
    queries_to_run =[queries[key] for key in config.sql_tables if key in queries]
    #print(queries_to_run)
    dir_name = os.path.split(os.path.split(dir.absolute())[0])[1]
    
    index_script_supply = """CREATE INDEX IF NOT EXISTS idx_supply_location_location ON location(location);
            CREATE INDEX IF NOT EXISTS idx_supply_location_zone ON location(zone);"""
    index_script_demand="""CREATE INDEX IF NOT EXISTS idx_demand_trip_person ON trip(person);
        CREATE INDEX IF NOT EXISTS idx_demand_activity_trip ON activity(trip);
        CREATE INDEX IF NOT EXISTS idx_demand_person_person ON person(person);
        CREATE INDEX IF NOT EXISTS idx_demand_person_household ON person(household);
        CREATE INDEX IF NOT EXISTS idx_demand_household_household ON household(household);
        CREATE INDEX IF NOT EXISTS idx_demand_household_location ON household(location);"""
    fix_trip_passengers = "update tnc_trip set passengers = 0  where request in (select tnc_request_id from tnc_request where service_type = 99);"
    fix_request_party_size = "update tnc_request set party_size = 0 where service_type = 99;"

    it = dir.as_posix().split("_")[-1]
    try:
        int(it)
    except:
        if dir.as_posix().endswith("_"):
            it = 0
    
    folder = dir_name + '_' + str(it)


    
    
    results = {}
    if len(config.sql_tables)>0 :
        #try:        
            with  sqlite3.connect(supply_db) as conn:
                ### Indexes for elder queries
                conn.cursor().executescript(index_script_supply)

            
            with sqlite3.connect(demand_db) as conn:
                temp_df = pd.read_sql(f"select * from tnc_request limit 10;", conn)
                if temp_df.shape[0] == 0:
                    print(f"No requests in {dir.as_posix()}.")
                    return {}
                conn.cursor().executescript(index_script_demand)
                conn.cursor().executescript(fix_trip_passengers)
                conn.cursor().executescript(fix_request_party_size)
                temp_df = pd.read_sql(f"select max(party_size) as max_party from tnc_request where service_type = 99;", conn)
                max_party = temp_df.iat[0,0]
                if max_party > 0:
                    raise ValueError(f"Adjustment of request party size did not take for {dir}")

                temp_df = pd.read_sql(f"select max(passengers) as max_pass from tnc_trip a left join tnc_request b on a.request = b.tnc_request_id where b.service_type = 99;", conn)
                max_pass = temp_df.iat[0,0]
                if max_pass > 0:
                    raise ValueError(f"Adjustment of trip passengers did not take for {dir}")
                temp_df = None

                for query in queries_to_run:
                    conn.cursor().executescript(query)
                for sql in config.sql_tables:
                    if sql in config.csvs.keys():
                        results[sql] = pd.read_sql(f"select * from {sql};", conn).assign(folder=folder)
                
                #####################################
                
                
        #except Exception as e:
          #  print(e)
          #  print(f"Database locked for db in path {dir}.")
           # conn.close()
           # raise
        ### postprocessing
        
    for key, details in config.csvs.items():
            if details['type']=="postprocessing" and not details["exists"]:
                for filename, (func_name, func_args) in config.postprocessing_definitions.items():
                    if key == filename:
                        
                        # Add the data argument to the function arguments
                        func_args['iter_dir'] = dir
                        func_args['folder']=folder
                        func_args['demand_db']=demand_db
                        func_args['supply_db']=supply_db
                        func_args['result_db']=result_db
                        func_args['trip_multiplier']=trip_multiplier
                        func_args['config']=config
                        func_args["request_file_name"] = "requests.csv"
                        # Call the function by name using globals()
                        print(f"Processing {filename} with {func_name} for {folder}.")
                        if func_name in globals():
                            df  = globals()[func_name](**func_args)
                            results[key]=df
                        else:
                            print(f"Function {func_name} not found.")
        
        
    print(f"Done: {dir_name}")
    return results
