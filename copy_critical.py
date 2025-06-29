import CU_POLARIS_Postprocessor
from CU_POLARIS_Postprocessor.utils import create_tasks, run_tasks, get_highest_iteration_folder
import os

old_case_path = r'/scratch/jpaul4/repositioning/rl_repo_data/convergence/'
new_case_path = r'/scratch/jpaul4/repositioning/rl_repo_data/for_offload_convergence'


moves={}
type1 = "copy"
type2 = "delete"
items1 =[]
items2 = []
folders_to_create = []
#add supply also in case it isn't there

for folder in os.listdir(old_case_path):
    old_fold_path = os.path.join(old_case_path,folder)
    
    if os.path.isdir(old_fold_path) and any("iteration" in name for name in os.listdir(old_fold_path)):
        highest_it = get_highest_iteration_folder(old_fold_path)
        for fold, _, files in os.walk(highest_it):
            rel_path_fold = os.path.relpath(fold,old_case_path)
            new_fold_path = os.path.join(new_case_path,rel_path_fold)
            folders_to_create.append(new_fold_path)
            for file in files:
                item_path = os.path.join(fold,file)
                new_item_path = os.path.join(new_fold_path,file)
                items1.append((item_path,new_item_path))
        if not any("Supply.sqlite" in file for file in os.listdir(highest_it)):
            for file in os.listdir(old_fold_path):
                if "-Supply.sqlite" in file:
                    supply_path = os.path.join(old_fold_path,file)
                    new_supply_path = os.path.join(new_fold_path,os.path.relpath(highest_it,old_fold_path),file)
                    items1.append((supply_path,new_supply_path))

    else: #items or folders that aren't iterations
        if os.path.isdir(os.path.join(old_case_path,folder)):
            new_fold_path = os.path.join(new_case_path,folder)
            folders_to_create.append(new_fold_path)
        else:
            items1.append((os.path.join(old_case_path,folder),os.path.join(new_case_path,folder)))

for fold in folders_to_create:
    if os.path.isdir(fold):
        continue
    else:
        os.makedirs(fold,exist_ok=False)

for item in items1:
    old = item[0]
    new = item[1]
    if os.path.isfile(old) and not os.path.isfile(new):
        continue
    else:
        if not os.path.isfile(old):
            skip = input(f"{old} doesn't exist, continue?[y,n]")
            if skip=="y":
                continue
            else:
                raise FileNotFoundError(f"{old} not found.")
        if os.path.isfile(new):
            skip = input(f"{new} already exists. Continue?[y,n]")
            if skip == "y":
                continue
            else:
                raise FileExistsError(f"{new} already exists.")
        


parallel = True
moves[type1] = items1
moves[type2] = items2
                

if len(moves) > 0:
        copy_tasks, del_tasks = create_tasks(moves)
        if len(copy_tasks)>0 or len(del_tasks)>0:
            run_tasks(parallel,copy_tasks,del_tasks)
    