import CU_POLARIS_Postprocessor
from CU_POLARIS_Postprocessor.utils import CaseVariableData, copy_cases
import os

case_path = r'/scratch/jpaul4/repositioning/rl_repo_data/base_models/atx_45000'
new_case_path=r'/scratch/jpaul4/repositioning/rl_repo_data/rate_testing/atx'
#new_case_path=r'/scratch/jpaul4/repositioning/rl_repo_data/comparisons/atx'

fleet_sizes = [i for i in range(30000,70000,10000)]
#fleet_sizes = [i for i in range(100,600,100)]
discount_rates = [i/10 for i in range (1, 8, 1)]


strategies =["nr","dr"] #for folder names
strat_key = {"nr":"config/TNCFleetModel_proactive_no_repo.json","dr":"config/TNCFleetModel_proactive_default_repo.json"} #corresponding keys for folder names (optional)

var_1 = CaseVariableData("fleet_size",[os.path.join("config","TNCFleetModel_joint_proactive_repo.json"),
                os.path.join("config","TNCFleetModel_proactive_no_repo.json"),
                os.path.join("config","TNCFleetModel_proactive_default_repo.json")],["Operator_1", "Fleet_Base", "TNC_FLEET_SIZE"],fleet_sizes)
#var_2 = CaseVariableData("strategy","scenario_abm.json",
#                ["ABM Controls","tnc_fleet_model_file"],strategies,strat_key)
var_2 = CaseVariableData("discount_rates",os.path.join("config","TNCFleetModel_joint_proactive_repo.json"),["DRS_Discount_AR","scaling_future_revenue"],discount_rates)
#var_2 = CaseVariableData("strategy","SAEVFleetModel_optimization.json",["Operator_1","Fleet Strategy", "Operator_1_strategy_name"],strategies, strat_key)

new_cases = [var_1,var_2]

args = {"new_case_path":new_case_path,
        "case_path":case_path,
        "new_cases":new_cases,
        "parallel":True,
        "move_cases":False}

copy_cases(**args)