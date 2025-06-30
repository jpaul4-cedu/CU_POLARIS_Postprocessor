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
    
    for dir in config.folder_db_map.keys():
        db_paths = config.folder_db_map[dir]
        supply_db = db_paths["supply"]
        demand_db = db_paths["demand"]
        result_db = db_paths['result']
        trip_multiplier = db_paths["trip_multiplier"]

        print(f"Cleaning: {dir}")
        for name in config.db_names:
            path = Path(supply_db)
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
                        demand_path = Path(demand_db)
                        if os.path.exists(demand_path):
                            if os.path.getsize(demand_path) < 1024:
                                os.remove(demand_path)
                        

                        
                    else:
                        continue


                break
        
        
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

    
