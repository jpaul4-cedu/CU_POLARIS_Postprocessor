from .utils import separate_keys_by_value, get_highest_iteration_folder, check_value_in_list
from .clean import clean_analysis
from .queries import get_sql_create
from pathlib import Path
import glob
import os
from .config import PostProcessingConfig
import pandas as pd



def pre_run_checks(config:PostProcessingConfig, skip_contains = None):
    # Validate there is a sql query for each sql based table we want
    
    #check if all the runs are complete
    path = config.base_dir.resolve()
    config.update_config(base_dir=path)
    parent_folder = config.base_dir
    folders = [Path(os.path.join(parent_folder, f)) for f in os.listdir(parent_folder) if (os.path.isdir(os.path.join(parent_folder, f)) and not f.endswith("_UNFINISHED"))]
    folders = [folder for folder in folders if all(_ not in folder.as_posix() for _ in config.ignore_folders) and not folder.as_posix() == config.analysis_folder]
    config.unique_folders = folders
    
    unfinished =[]
    for folder in config.unique_folders:
        subdirs = os.listdir(folder)
        if any("iteration" in sub_dir for sub_dir in subdirs):
            dir = get_highest_iteration_folder(folder)
            if os.path.exists(os.path.join(dir,"finished")):
                continue
        unfinished.append(folder)

            

    if len(unfinished) > 0:
        for item in unfinished:
            config.unfinished.append(item)
        return False



    
    cats = separate_keys_by_value(config.desired_outputs)
    sql_cats = cats['sql']

    queries = get_sql_create(supply_db='dummy',trip_multiplier='dummy',result_db='dummy')
    missing_items = [item for item in sql_cats if item not in queries]
        
    if missing_items:
        raise ValueError(f"Missing entries in dictionary for items: {', '.join(missing_items)}")
    # remove existing data tables 
    
    sql_tables=[]
    output_csvs_exists = {}
    results = {}
    csvs_exist = True
    results = {}
    folders = []
        
    skip_items = []
    if not skip_contains is None:
        print(f"Skipping folders with names containing {skip_contains}")
        for item in config.unique_folders:
            if skip_contains in item.as_posix():
                skip_items.append(item)
        
    
    for outer_key, items in cats.items():
        if outer_key == 'sql_helper':
            for item in items:
                sql_tables.insert(0,item)        
        elif outer_key in ['sql', 'postprocessing']:
            for item in items:
                csv_path = Path(config.analysis_folder + "/" + item + ".csv")
                if item not in output_csvs_exists:
                    output_csvs_exists[item] = {}
                output_csvs_exists[item]['type'] = outer_key
                output_csvs_exists[item]['path'] = csv_path
                if not os.path.exists(csv_path):
                    output_csvs_exists[item]['exists'] = False
                    if outer_key=='sql':
                        sql_tables.append(item)
                    #print(f"{csv_path} main output not found, will continue to processing.")
                else:
                    output_csvs_exists[item]['exists'] = True
                    df = pd.read_csv(csv_path)
                    folders.append(df['folder'].unique().tolist())
                    results[item]=pd.read_csv(csv_path)
                
        elif outer_key == 'postprocessing_helper':
            for item in items:
                for dir in config.unique_folders:
                    iter_dir = get_highest_iteration_folder(dir)
                    try:
                        csv_path = Path(config.analysis_folder + '/' + item + '.csv')
                        dir_name = os.path.split(os.path.split(iter_dir.absolute())[0])[1]
                    except:
                        raise FileNotFoundError(f"The folder, {dir}, does not appear to have the correct iteration folder. Please remove any non-case folders from the analysis directory.")
                    helper_item = item+'_'+dir_name
                    if helper_item not in output_csvs_exists:
                        output_csvs_exists[helper_item] = {}
                    output_csvs_exists[helper_item]['type'] = outer_key
                    output_csvs_exists[helper_item]['path'] = csv_path
                    if not os.path.exists(csv_path):
                        output_csvs_exists[helper_item]['exists'] = False
                        #print(f"{csv_path} helper output not found, will continue to processing.")
                    else:
                        output_csvs_exists[helper_item]['exists'] = True

    list_of_tuples = [tuple(lst) for lst in folders]

    # Get unique tuples
    unique_tuples = set(list_of_tuples)

    # Convert back to list of lists (if needed)
    unique_folders = [list(tpl) for tpl in unique_tuples]    
    unique_folders = [item for sublist in unique_folders for item in sublist]
    if len(unique_folders)>0:
        for dir in config.unique_folders:
            try:
                name = dir.name
                result = check_value_in_list(name,unique_folders)
            except ValueError as e:
                print(e)
                raise
        # remove helper queries if all
    config.update_config(csvs=output_csvs_exists)
    config.update_config(sql_tables=sql_tables)
    #config.update_config(unique_folders=unique_folders)
    config.update_config(results=results)
    all_csvs_exist = True
    for key, value in output_csvs_exists.items():
        if (not value['exists']) and (value['type'] == 'sql'):
            all_csvs_exist = False
            break
    if all_csvs_exist: 
        sql_tables=[]
    if config.fresh_start:
        for outer_key, items in cats.items():
            if outer_key == 'sql_helper':
                for item in items:
                    if not item == "attach":
                        sql_tables.insert(0,item)        
            elif outer_key in ['sql', 'postprocessing']:
                for item in items:
                    if outer_key=='sql':
                        sql_tables.append(item)
        config.update_config(sql_tables=sql_tables)
        clean_analysis(config)
        config.update_config(fresh_start=False)

        
        return pre_run_checks(config)
    else:
        return True

