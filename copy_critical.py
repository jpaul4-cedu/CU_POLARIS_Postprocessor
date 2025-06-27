import CU_POLARIS_Postprocessor
from CU_POLARIS_Postprocessor.utils import create_tasks, run_tasks, get_highest_iteration_folder
import os

old_case_path = r'/scratch/jpaul4/repositioning/rl_repo_data/for_anal/'
new_case_path = r'/scratch/jpaul4/repositioning/rl_repo_data/for_offload/'


moves={}
type1 = "copy"
type2 = "delete"
items1 =[]
items2 = []


for fold in os.listdir(old_case_path):
    fold_path = os.path.join(old_case_path, fold)
    if os.path.isfile(fold_path):
        items1.append((fold_path,new_case_path))
        continue
    
    iteration = get_highest_iteration_folder(fold_path)
    if iteration is None:
        items1.append((fold_path,new_case_path))
        continue
    if "finished" in os.listdir(iteration) or "UNFINISHED" in fold_path:
        os.makedirs(os.path.join(new_case_path,fold),exist_ok=True)
        items1.append((iteration,os.path.join(new_case_path,fold, os.path.split(iteration)[-1])))
    else:
        raise FileNotFoundError(f"Latest iteration in {fold_path} has no finished file.")
    

parallel = True
moves[type1] = items1
moves[type2] = items2
                
        



if len(moves) > 0:
        copy_tasks, del_tasks = create_tasks(moves)
        if len(copy_tasks)>0 or len(del_tasks)>0:
            run_tasks(parallel,copy_tasks,del_tasks)
    