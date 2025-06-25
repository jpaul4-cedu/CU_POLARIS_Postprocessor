from CU_POLARIS_Postprocessor.utils import select_file, bulk_update_scenario_files, copy_cases, CaseVariableData, BulkUpdateData, bulk_check_scenario_files
from CU_POLARIS_Postprocessor.utils import create_jobscript, CityKey, PolarisRunConfig, MailType
from pathlib import Path

'''
checks ={"SAEVFleetModel_optimization.json":['Operator_1','Fleet_DRS','Operator_1_pool_choice_enabled'],
"scenario_abm.json":["ABM Controls","tnc_fleet_model_file"],
"SAEVFleetModel_optimization.json":["Operator_1", "Fleet_Base", "Operator_1_TNC_FLEET_SIZE"],
"SAEVFleetModel_optimization.json":["Operator_1","Fleet Strategy", "Operator_1_strategy_name"]}
'''
checks =[("config/TNCFleetModel_proactive_default_repo.json",["Operator_1","Fleet_Base", "TNC_FLEET_SIZE"]),
            ("scenario_abm_jar.json",["ABM Controls","tnc_fleet_model_file"]),
            ("config/TNCFleetModel_joint_proactive_repo.json",["DRS_Discount_AR","scaling_future_revenue"])]


new_case_path=r'/scratch/jpaul4/repositioning/rl_repo_data/comparisons/atx'
folder_paths = [Path("/scratch/jpaul4/repositioning/rl_repo_data/rate_testing/atx")]



for folder_path in folder_paths:
    for scenario_names, target in checks:
        bulk_check_scenario_files(target, folder_path, [scenario_names])
