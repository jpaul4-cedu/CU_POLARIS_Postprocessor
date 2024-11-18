
from CU_POLARIS_Postprocessor.config import PostProcessingConfig
from CU_POLARIS_Postprocessor import prerun, parallel
from CU_POLARIS_Postprocessor.power_bi_processing import prep_utils
from pathlib import Path
import importlib as IL
import CU_POLARIS_Postprocessor
import CU_POLARIS_Postprocessor.power_bi_processing
import CU_POLARIS_Postprocessor.power_bi_processing.prep_utils

config_wtp=PostProcessingConfig(base_dir=Path("C://Globus//mode_choice_cases"),
                                output_h5=True,fresh_start=False,reset_csvs=False,reset_sql=False,reset_stops=False, db_names=['Austin'])

prerun.pre_run_checks(config_wtp)

import pandas as pd
csvs = config_wtp.csvs
demo = []
for key, value in csvs.items():
        # Extract the values for each column
        type_val = value.get('type', None)
        exists_val = value.get('exists', None)
        location_val = value.get('path', None)
        
        # Append the row as a tuple
        demo.append((key, type_val, exists_val, location_val))

    # Create a DataFrame from the rows
df = pd.DataFrame(demo, columns=['Key', 'Type', 'Exists', 'Location'])
df

parallel.parallel_process_folders(config_wtp)
IL.reload(CU_POLARIS_Postprocessor.power_bi_processing.prep_utils)
#use this for comparing all cases (within a city model) to a single base case
base_case_folders = ['atx_du_7','gvl_du_7']
#prep_utils.process_tnc_ttests(config=config_wtp,base_cases=base_case_folders)

#Use this for comparing strategy to strategy
prep_utils.process_tnc_ttests(config=config_wtp,base_suffix="_heur")

#Use this for folder names structured cit_fleet-size_strategy_iteration
prep_utils.process_folder_names_cit_fleet_strat(config_wtp)

#Use this for folder names structured cit_case_iteration
#prep_utils.process_folder_names_wtp(config_wtp)

case_aggregators =["City","Strategy"]
demo_aggregators =["age_class"]
folder_to_columns = ["City","Strategy","Iteration","Fleet Size"]
prep_utils.generate_pbix_control_csv(config_wtp,case_aggregators,demo_aggregators,folder_to_columns)
prep_utils.update_h5(config=config_wtp)