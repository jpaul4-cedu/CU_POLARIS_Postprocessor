from CU_POLARIS_Postprocessor.utils import CityKey, PolarisRunConfig,create_jobscript, MailType
import os

#redoing the cases that failed
analysis_path = r"/scratch/jpaul4/repositioning/rl_repo_data/for_anal"
redo_folds = [path.replace("_UNFINISHED","") for path in os.listdir(analysis_path) if os.path.isdir(os.path.join(analysis_path,path)) and path.endswith("_UNFINISHED")]
custom_foot = \
f"""cd "$SCENARIO_PATH"\napptainer exec --bind "$SCENARIO_PATH":"$SCENARIO_PATH" /home/jpaul4/new_container/rl_repo_polaris.sif /app/polaris/polaris_clemson/polaris-linux/build/linux/gcc/release/bin/Integrated_Model scenario_abm.json 20"""
gsc = CityKey("greenville","gsc",1.0,16,3.0) #extended time


#Citykey(model_database_name(<model_name>-Suppy.sqlite),folder prefix, traffic scale factor, memory per run(gb), time per run(hours))
#case_dir = r"/scratch/jpaul4/repositioning/rl_repo_data/comparisons/atx"
case_dir = analysis_path
#gsc = CityKey("greenville","gsc",1.0,16,1.5)


atx = CityKey("Austin","atx",0.25,80,3.5)


#PolarisRunConfig(container_path, run script (internal to container), threads, do abm init run, do skim run, number of abm runs, cities (CityKey objects))
run_config = PolarisRunConfig("/home/jpaul4/new_container/polaris_container_dev","run_old_polaris.py",32,False,False,1,[gsc,atx])
#create_jobscript(job_name="polaris_rl_pop_synth",mail_user="jpaul4@clemson.edu",mail_type=MailType.ALL,case_dir=case_dir,config=run_config, redo_files=redo_folds,custom_foot=custom_foot) 
create_jobscript(job_name="polaris_rl_pop_synth",mail_user="jpaul4@clemson.edu",mail_type=MailType.ALL,case_dir=case_dir,config=run_config, custom_foot=custom_foot) 