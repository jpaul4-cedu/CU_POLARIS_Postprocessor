from pathlib import Path
from .utils import get_highest_iteration_folder
import os
import subprocess
import sqlite3
import glob
from CU_POLARIS_Postprocessor.config import PostProcessingConfig


def clean_analysis(config:PostProcessingConfig):
    run_dirs = [Path(d) for d in glob.glob(config.base_dir.as_posix() + "/*/")]
    for dir in run_dirs:
        dir = get_highest_iteration_folder(dir)
        
        print(f"Cleaning: {dir}")
        for name in config.db_names:
            if os.path.exists(Path(dir.as_posix() + '/' + name+'-Supply.sqlite')):
                db_name = name
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
            cmd = f" tar xf {supply_db}.tar.gz"
            subprocess.check_output(cmd, shell=True, encoding="utf-8")
        
        
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
