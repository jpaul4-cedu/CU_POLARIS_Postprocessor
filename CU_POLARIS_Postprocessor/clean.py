from pathlib import Path
from CU_POLARIS_Postprocessor.utils import get_highest_iteration_folder
import os
import subprocess
import sqlite3
import glob
from CU_POLARIS_Postprocessor.config import PostProcessingConfig
import tkinter as tk
from tkinter import messagebox
import shutil


def clean_analysis(config:PostProcessingConfig):
    '''Clean analysis loops through all of the case folders 
    in an output dataset and removes the desired existing analysis CSVs, 
    SQL Tables, and any other supporting files to provide a fresh start for analysis.'''
    run_dirs = [Path(d) for d in glob.glob(config.base_dir.as_posix() + "/*/")]
    for dir in run_dirs:
        dir = get_highest_iteration_folder(dir)
        
        print(f"Cleaning: {dir}")
        for name in config.db_names:
            path = Path(dir.as_posix() + '/' + name+'-Supply.sqlite')
            if os.path.exists(path):
                if os.path.getsize(path) > 0:
                    db_name = name
                elif os.path.getsize(path) < 1024:
                    root = tk.Tk()
                    root.overrideredirect(1)
                    root.withdraw()
                    response = messagebox.askyesno("Empty DB Found", f"The supply db at {path} appears to be empty. Would you like to delete it?")
                    root.destroy()
                    if response:
                        os.remove(path)
                        demand_path = Path(dir.as_posix() + '/' + name+'-Demand.sqlite')
                        if os.path.exists(demand_path):
                            if os.path.getsize(demand_path) < 1024:
                                os.remove(demand_path)
                        

                        
                    else:
                        continue


                break
        
        os.chdir(dir)
        demand_db = db_name + "-Demand.sqlite"
        #result_db = f"{db_name}-Result.sqlite"
        result_db = db_name + "-Result.sqlite"
        supply_db = db_name + "-Supply.sqlite"
        
        
        if not os.path.exists(demand_db):
            cmd = f" tar xf {demand_db}.tar.gz"
            subprocess.check_output(cmd, shell=True, encoding="utf-8")

        if not os.path.exists(result_db):
            cmd = f" tar xf {result_db}.tar.gz"
            subprocess.check_output(cmd, shell=True, encoding="utf-8")
    
        if not os.path.exists(supply_db):
            if not os.path.exists(dir.parent.as_posix() + '/' + supply_db):
                cmd = f" tar xf {supply_db}.tar.gz"
                subprocess.check_output(cmd, shell=True, encoding="utf-8")
            else:
                shutil.copyfile(dir.parent.as_posix() + '/' + supply_db, supply_db)
        
        
        if config.reset_sql:   
            with sqlite3.connect(demand_db) as conn:
                for tab in config.sql_tables:
                    conn.cursor().executescript(f"""drop table if exists {tab};""")
    csvs = []
    for item, details in config.csvs.items():
            if details['exists'] == True:
                if details['type'] == "postprocessing_helper" and config.reset_stops:
                    csvs.append(details['path'])
                if config.reset_csvs:
                    csvs.append(details['path'])
    
    for csv in csvs:
            if os.path.exists(csv):
                os.remove(csv)
                print(f"File {csv} has been deleted successfully.")
            else:
                print(f"The file {csv} does not exist.")

    os.chdir(dir)
