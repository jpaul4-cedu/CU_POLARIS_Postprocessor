from CU_POLARIS_Postprocessor.utils import CityKey, PolarisRunConfig,create_jobscript, MailType

#Citykey(model_database_name(<model_name>-Suppy.sqlite),folder prefix, traffic scale factor, memory per run(gb), time per run(hours))
case_dir = r"/scratch/jpaul4/repositioning/rl_repo_data/rate_testing/gsc"
gsc = CityKey("greenville","gsc",1.0,16,1.5)
atx = CityKey("Austin","atx",0.25,80,3.5)

#PolarisRunConfig(container_path, run script (internal to container), threads, do abm init run, do skim run, number of abm runs, cities (CityKey objects))
run_config = PolarisRunConfig("/home/jpaul4/new_container/polaris_container_dev","run_old_polaris.py",32,False,False,7,[gsc,atx])
create_jobscript(job_name="polaris_trb_cases",mail_user="jpaul4@clemson.edu",mail_type=MailType.ALL,case_dir=case_dir,config=run_config) 