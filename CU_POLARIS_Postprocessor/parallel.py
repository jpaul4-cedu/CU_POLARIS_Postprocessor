import concurrent.futures
from pathlib import Path
import os
from .utils import get_highest_iteration_folder, get_scale_factor
from .queries import get_sql_create
import tarfile
import re
import sqlite3
from .config import PostProcessingConfig
import pandas as pd
import itertools
import json
from .postprocessing import process_batch_nearest_stops, process_elder_request_agg, process_nearest_stops, process_solo_equiv_fare, process_tnc_stat_summary

def parallel_process_folders(config:PostProcessingConfig):
    # Get a list of all subfolders in the parent folder
    parent_folder = config.base_dir
    folders = [Path(os.path.join(parent_folder, f)) for f in os.listdir(parent_folder) if os.path.isdir(os.path.join(parent_folder, f))]

    # Use ThreadPoolExecutor or ProcessPoolExecutor to process folders in parallel
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(executor.map(process_folder, folders, itertools.repeat(config)))

    collected_results={}

    for result in results:
        if not len(result) == 0:
            for key, df in result.items():
                if key not in collected_results:
                    collected_results[key]=[]
                collected_results[key].append(df)
    final_results = {key: pd.concat(df_list, ignore_index=True) for key, df_list in collected_results.items()}
    for key, df in final_results.items():
        df.to_csv(parent_folder.as_posix() +'/' + key + '.csv', index=False)
    final_results.update(config.results)
    config.update_config(results=final_results)
    if config.output_h5:
        with pd.HDFStore(config.base_dir.as_posix()+'/results.h5') as store:
            for key, df in final_results.items():
                store[key] = df
    return True

def process_folder(dir, config:PostProcessingConfig):

    print(f"starting on directory {dir}")
    dir = get_highest_iteration_folder(dir)
    #dir = Path(dir.as_posix())
    #print(f"Opening: {dir}")
    for name in config.db_names:
        if os.path.exists(Path(dir.as_posix() + '/' + name+'-Supply.sqlite')):
            db_name = name
            break
        elif os.path.exists(Path(dir.as_posix() + '/' + name+'-Supply.sqlite.tar.gz')):
            db_name = name
            break
    
    
    demand_db = db_name + "-Demand.sqlite"
    #result_db = f"{db_name}-Result.sqlite"
    result_db = db_name + "-Result.sqlite"
    supply_db = db_name + "-Supply.sqlite"
    trip_multiplier = get_scale_factor(dir,config)
    
    
    if not os.path.exists(dir.as_posix() + '/'+ demand_db):
        with tarfile.open(dir.as_posix()+'/' + demand_db + '.tar.gz','r:gz') as tar:
            tar.extractall(path = dir)     
        

    if not os.path.exists(dir.as_posix() + '/'+ result_db):
        with tarfile.open(dir.as_posix()+'/' + result_db + '.tar.gz','r:gz') as tar:
            tar.extractall(path = dir)

    if not os.path.exists(dir.as_posix() + '/'+ supply_db):
        with tarfile.open(dir.as_posix()+'/' + supply_db + '.tar.gz','r:gz') as tar:
            tar.extractall(path = dir)


    queries = get_sql_create(supply_db=dir.as_posix() + '/'+ supply_db,trip_multiplier=trip_multiplier,result_db=dir.as_posix() + '/'+result_db)
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
    
    match = re.search(r'_iteration_(\d+)', dir.as_posix())
    it_num =  int(match.group(1))
    folder = dir_name+'_' +str(it_num)
    
    results = {}
    if len(config.sql_tables)>0 :
        try:        
            with  sqlite3.connect(dir.as_posix() + '/'+ supply_db) as conn:
                ### Indexes for elder queries
                conn.cursor().executescript(index_script_supply)

            
            with sqlite3.connect(dir.as_posix() + '/'+ demand_db) as conn:
                conn.cursor().executescript(index_script_demand)
                for query in queries_to_run:
                    conn.cursor().executescript(query)
                for sql in config.sql_tables:
                    if sql in config.csvs.keys():
                        results[sql] = pd.read_sql(f"select * from {sql};", conn).assign(folder=folder)
                
                #####################################
                
                
        except:
            print(f"Database locked for db in path {dir}.")
            conn.close()
            raise
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
                        # Call the function by name using globals()
                        print(f"Processing {filename} with {func_name} for {folder}.")
                        if func_name in globals():
                            df  = globals()[func_name](**func_args)
                            results[key]=df
                        else:
                            print(f"Function {func_name} not found.")
        
        
    print(f"Done: {dir_name}")
    return results
