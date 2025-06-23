
import importlib as IL
import os
import CU_POLARIS_Postprocessor
from CU_POLARIS_Postprocessor.config import PostProcessingConfig
from CU_POLARIS_Postprocessor import prerun, parallel
from CU_POLARIS_Postprocessor.power_bi_processing import prep_utils
from pathlib import Path
import importlib as IL
import CU_POLARIS_Postprocessor
import CU_POLARIS_Postprocessor.power_bi_processing
import CU_POLARIS_Postprocessor.power_bi_processing.prep_utils
import CU_POLARIS_Postprocessor.power_bi_processing.charting as pbix_charting
IL.reload(CU_POLARIS_Postprocessor.power_bi_processing.prep_utils)


def run_anal(config_wtp:PostProcessingConfig):
    prerun.pre_run_checks(config_wtp)
    parallel.parallel_process_folders(config_wtp)
    IL.reload(CU_POLARIS_Postprocessor.power_bi_processing.prep_utils)
    #use this for comparing all cases (within a city model) to a single base case
    base_case_folders = ['gsc_300_jar','gvl_du_7']
    #prep_utils.process_tnc_ttests(config=config_wtp,base_cases=base_case_folders)

    #Use this for comparing strategy to strategy
    prep_utils.process_tnc_ttests(config=config_wtp,base_suffix="_jar")

    #Use this for folder names structured cit_fleet-size_strategy_iteration
    prep_utils.process_folder_names_cit_fleet_strat(config_wtp)

    #Use this for folder names structured cit_case_iteration
    #prep_utils.process_folder_names_wtp(config_wtp)

    case_aggregators =["City","Strategy"]
    demo_aggregators =["age_class"]
    folder_to_columns = ["City","Strategy","Iteration","Fleet Size"]
    prep_utils.generate_pbix_control_csv(config_wtp,case_aggregators,demo_aggregators,folder_to_columns)
    prep_utils.update_h5(config=config_wtp)

def run_pbix(config_wtp:PostProcessingConfig):
    pbix_aggregators  = {
    'Transit Aggregators': ['City', 'Strategy'],
    'Folder to Columns': ['City', 'Strategy','Fleet Size'],
    'Demographic Aggregators':['City','age_class'],
    'Case Aggregators': ['City', 'Fleet Size'],
    'Strategy Aggregators': ['City', 'Strategy'],
    'ttest_aggregators': ['City', 'base_case']
    }
    work_path = config_wtp.base_dir
    db_name_folder_map = {
        'greenville':'gsc',
        'campo':'atx'
    }
    study_name = 'rl_repo'
    #generates a new h5 file called pbix_tables.h5 which are directly used to generate charts in power bi
    pbix_charting.run_all(work_path,pbix_aggregators,db_name_folder_map,study_name,db_path=({"greenville":r"C:\polaris\data\tests_local\base_models\gsc_300\greenville-Supply.sqlite","campo":r"C:\polaris\data\base_models_new_polaris\atx_27500\Austin-Supply.sqlite"}))
    pbix_charting.open_pbix_file(config_wtp.base_dir,study_name)

def __main__():
    config_wtp=PostProcessingConfig(base_dir=Path(r"C:\\polaris\\data\\tests_local\\initial_tests"),
                                    output_h5=True,
                                    fresh_start=True,
                                    reset_csvs=True,
                                    reset_sql=True,
                                    reset_stops=True, 
                                    db_names=['greenville','campo'],
                                    scenario_file_names=['scenario_abm_jar.json','scenario_abm_default_repo.json','scenario_abm_no_repo.json'],
                                    fleet_model_file_names=['TNCFleetModel_joint_proactive_repo.json','TNCFleetModel_proactive_default_repo.json','TNCFleetModel_proactive_no_repo.json'],
                                    parallel=False)
    run_anal(config_wtp)
    run_pbix(config_wtp)
   # pbix_charting.create_pbix_path_query()

__main__()