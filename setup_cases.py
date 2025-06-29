import CU_POLARIS_Postprocessor
from CU_POLARIS_Postprocessor.utils import CaseVariableData, copy_cases, bulk_check_scenario_files, CityKey, create_jobscript, PolarisRunConfig, MailType
import os

case_path_atx = r'/scratch/jpaul4/repositioning/rl_repo_data/base_models/atx_45000'
case_path_gsc = r'/scratch/jpaul4/repositioning/rl_repo_data/base_models/gsc_300'
new_case_path=r'/scratch/jpaul4/repositioning/rl_repo_data/convergence/'
#new_case_path=r'/scratch/jpaul4/repositioning/rl_repo_data/comparisons/atx'

fleet_sizes_atx = [i for i in range(30000,75000,15000)]
fleet_sizes_gsc = [i for i in range(200,800,200)]
discount_rates = [i/10 for i in range (2, 10, 2)]


strategies =["nr","dr"] #for folder names
strat_key = {"nr":"config/TNCFleetModel_proactive_no_repo.json","dr":"config/TNCFleetModel_proactive_default_repo.json"} #corresponding keys for folder names (optional)

var_1 = CaseVariableData("fleet_size",[os.path.join("config","TNCFleetModel_joint_proactive_repo.json"),
                os.path.join("config","TNCFleetModel_proactive_no_repo.json"),
                os.path.join("config","TNCFleetModel_proactive_default_repo.json")],["Operator_1", "Fleet_Base", "TNC_FLEET_SIZE"],fleet_sizes_atx)

var_2 = CaseVariableData("fleet_size",[os.path.join("config","TNCFleetModel_joint_proactive_repo.json"),
                os.path.join("config","TNCFleetModel_proactive_no_repo.json"),
                os.path.join("config","TNCFleetModel_proactive_default_repo.json")],["Operator_1", "Fleet_Base", "TNC_FLEET_SIZE"],fleet_sizes_gsc)

var_3 = CaseVariableData("strategy","scenario_abm.json",
         ["ABM Controls","tnc_fleet_model_file"],strategies,strat_key)
var_4 = CaseVariableData("discount_rates",os.path.join("config","TNCFleetModel_joint_proactive_repo.json"),["DRS_Discount_AR","scaling_future_revenue"],discount_rates)


cases = []
cases.append((case_path_atx,[var_1,var_4]))
cases.append((case_path_atx,[var_1,var_3]))
cases.append((case_path_gsc,[var_2,var_4]))
cases.append((case_path_gsc,[var_2,var_3]))

for case in cases:
      args = {"new_case_path":new_case_path,
            "case_path":case[0],
            "new_cases":case[1],
            "parallel":True,
            "move_cases":False}

      copy_cases(**args)



#bulk1=BulkUpdateData(possible_scenario_names=[os.path.join("config","TNCFleetModel_joint_proactive_repo.json"),
 #               os.path.join("config","TNCFleetModel_proactive_no_repo.json"),
  #              os.path.join("config","TNCFleetModel_proactive_default_repo.json")],value=50.0,target=['DRS_Discount_AR','DRS_MAX_PERC_DELAY'])
#bulk2=BulkUpdateData(possible_scenario_names=["scenario_abm.json"],value=0.25,target=['ABM Controls','traffic_scale_factor'])
#bulk1=BulkUpdateData(possible_scenario_names=["TNCFleetModel_proactive_default_repo.json"],value=0.5,target=['Operator_1','Fleet_DRS','Operator_1_pool_choice_enabled'])
#bulk2=BulkUpdateData(possible_scenario_names=["scenario_abm.json"],value="SAEVFleetModel_optimization.json",target=["ABM Controls","tnc_fleet_model_file"])

#updates = [bulk1,bulk2]

#for update in updates:
 #   bulk_update_scenario_files(update.target,"atx_", update.value,new_case_path,update.scenario_name)


checks =[("config/TNCFleetModel_proactive_default_repo.json",["Operator_1","Fleet_Base", "TNC_FLEET_SIZE"]),
            ("scenario_abm.json",["ABM Controls","tnc_fleet_model_file"]),
            ("config/TNCFleetModel_joint_proactive_repo.json",["DRS_Discount_AR","scaling_future_revenue"]),
            ("config/TNCFleetModel_proactive_no_repo.json",["Operator_1","Fleet_Base", "TNC_FLEET_SIZE"]),
            ("config/TNCFleetModel_joint_proactive_repo.json",["Operator_1","Fleet_Base", "TNC_FLEET_SIZE"]),
            ("config/TNCFleetModel_joint_proactive_repo.json",['DRS_Discount_AR','DRS_MAX_PERC_DELAY']),
            ("config/TNCFleetModel_proactive_no_repo.json",['DRS_Discount_AR','DRS_MAX_PERC_DELAY']),
            ("config/TNCFleetModel_joint_proactive_repo.json",['DRS_Discount_AR','DRS_MAX_PERC_DELAY'])
            ]

folder_paths = [new_case_path]



for folder_path in folder_paths:
    for scenario_names, target in checks:
        bulk_check_scenario_files(target, folder_path, [scenario_names])
          


conf = input("Confirm cases set up?[y,n]")
if conf == "y":
      pass
else:
      raise ValueError("Cases not set up correctly.")

case_dir = new_case_path
gsc = CityKey("greenville","gsc",1.0,16,1.5)
atx = CityKey("Austin","atx",0.25,80,3.5)
run_config = PolarisRunConfig("/home/jpaul4/new_container/rl_repo_polaris.sif","/mnt/data/run_polaris.py",32,True,False,7,[gsc,atx])
create_jobscript(job_name="polaris_rl_pop_synth",mail_user="jpaul4@clemson.edu",mail_type=MailType.ALL,case_dir=case_dir,config=run_config) 
