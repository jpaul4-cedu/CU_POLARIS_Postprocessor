from CU_POLARIS_Postprocessor.utils import create_tasks, run_tasks, execute_task
from threading import Thread
import os
from concurrent import futures


def queue_deletes(dels: list, dir:str):
    moves ={}
    moves["delete"]=dels
    _, tasks = create_tasks(moves, force = True)
    run_tasks(del_tasks=tasks, use_tqdm=False, max_threads_x2 = True)
    fold_delete = f"rm -rf {dir}"
    execute_task(fold_delete)
    print(f"removed {dir}")



root_dir = "/home/jpaul4/new_container/sandbox_container/opt/polaris/deps/"
i = 0
max_workers = os.cpu_count()

with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
    for dirpath, _, filenames in os.walk(root_dir,topdown=False):
        queue =[]
        for filename in filenames:
            full_path = os.path.join(dirpath, filename)
            queue.append(full_path)
        executor.submit(queue_deletes, queue,dirpath)
    

