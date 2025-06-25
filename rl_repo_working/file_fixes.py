from CU_POLARIS_Postprocessor.utils import create_tasks, run_tasks, copy_cases, bulk_update_scenario_files, BulkUpdateData
import os
import shutil

def delete_runs ():
    type1 = "copy"
    type2 = "delete"
    items1 =[]
    items2 = []
    parallel = True
    moves = {}
    new_case_path = r"/scratch/jpaul4/repositioning/rl_repo_data/comparisons/atx/"
    if os.path.isdir(new_case_path):
        all_exist = True
        for fold in os.listdir(new_case_path):
            fold_path = os.path.join(new_case_path, fold)
            if os.path.isfile(fold_path):
                pass
            elif os.path.isdir(fold_path):
                for subfold in os.listdir(fold_path):
                    sub_fold_path = os.path.join(fold_path, subfold)
                    if "Austin" in subfold and os.path.isdir(sub_fold_path):
                        items2.append(sub_fold_path)
                
        
            moves[type1] = items1
            moves[type2] = items2
    return moves

def move_runs():
    move_cases = True

    #keep_suffixes = ['reg'] #end of folder name to move (if you only want to move some cases)
    keep_suffixes = None #move all cases

    new_case_path = "/scratch/jpaul4/repositioning/rl_repo_data/for_anal/"
    case_paths = [r"/scratch/jpaul4/repositioning/rl_repo_data/comparisons/atx/",
                            r"/scratch/jpaul4/repositioning/rl_repo_data/comparisons/gsc/",
                            r"/scratch/jpaul4/repositioning/rl_repo_data/rate_testing/atx/",
                            r"/scratch/jpaul4/repositioning/rl_repo_data/rate_testing/gsc/"]

    for case_path in case_paths:
            args = {"new_case_path":new_case_path,
                    "case_path":case_path,
                    "move_cases":move_cases,
                    "keep_suffixes":keep_suffixes,
                    "parallel":True,
                    "include_dirs":True} #use parallel processing (faster), not necessary for small number of cases or small models

            copy_cases(**args)

#moves = move_runs()
""" new_case_path = "/scratch/jpaul4/repositioning/rl_repo_data/for_anal/"
    
for fold in os.listdir(new_case_path):
    if not "dr" in fold and not "nr" in fold and "run" not in fold:
        path = os.path.join(new_case_path,fold)
        shutil.copy(os.path.join(path,"scenario_abm_jar.json"),os.path.join(path,"scenario_abm.json"))
 """
bulk1=BulkUpdateData(possible_scenario_names=["scenario_abm.json"],value="austin_iteration_",target=['Output controls','output_directory'])
bulk2=BulkUpdateData(possible_scenario_names=["scenario_abm.json"],value=0.25,target=['ABM Controls','traffic_scale_factor'])


#bulk1=BulkUpdateData(possible_scenario_names=["SAEVFleetModel_optimization.json"],value=True,target=['Operator_1','Fleet_DRS','Operator_1_pool_choice_enabled'])
#bulk2=BulkUpdateData(possible_scenario_names=["scenario_abm.json"],value="SAEVFleetModel_optimization.json",target=["ABM Controls","tnc_fleet_model_file"])

updates = [bulk1,bulk2]
#updates = [bulk]
new_case_path = "/scratch/jpaul4/repositioning/rl_repo_data/for_anal/"
for update in updates:
    bulk_update_scenario_files(update.target,"atx_", update.value,new_case_path,update.scenario_name)


""" if len(moves) > 0:
    copy_tasks, del_tasks = create_tasks(moves)
    if len(copy_tasks)>0 or len(del_tasks)>0:
        run_tasks(parallel,copy_tasks,del_tasks)
 """