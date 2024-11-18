from CU_POLARIS_Postprocessor.utils import bulk_update_scenario_files, copy_cases
from pathlib import Path
import os
import shutil
import subprocess


case_path = Path(r'C:\Globus\mode_choice_cases\atx_27500')

dest_path = Path(r"C:\Globus")
new_case_path = os.path.join(dest_path,'trb_cases/atx')

keep_files =['SAEVFleetModel_optimization.json']
move_cases = False

keep_suffixes = ['reg'] #end of folder name to move

check_city = True


copy_cases(new_case_path,case_path,move_cases,keep_files=keep_files,keep_suffixes=keep_suffixes, parallel=True, check_city=check_city)

                        

json_level_path = ['Operator_1','Fleet_Base','delivery_type']
val = "person"
bulk_update_scenario_files(json_level_path,val,new_case_path,["SAEVFleetModel_optimization.json"])