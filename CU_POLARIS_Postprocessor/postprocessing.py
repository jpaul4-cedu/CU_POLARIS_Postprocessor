import os
import sqlite3
import pandas as pd
import numpy as np
from .utils import get_timeperiods, get_tnc_pricing, get_heur_discount
import openmatrix as omx
from CU_POLARIS_Postprocessor.config import PostProcessingConfig
from CU_POLARIS_Postprocessor.utils import get_scale_factor
from functools import partial

def process_nearest_stops(iter_dir, folder, **kwargs):
    dir = iter_dir
    from sklearn.neighbors import BallTree
    from joblib import Parallel, delayed
    print(f"Starting stop processing for {dir}.")
    read_house_sql ="""SELECT b.household, c.x, c.y, b.workers, b.vehicles,
    COUNT(CASE WHEN a.age > 60 THEN 1 END) AS people_over_60,
    COUNT(CASE WHEN a.age <= 60 THEN 1 END) AS people_under_60,
    sum(a.income) as income, c.zone
    FROM person a
    LEFT JOIN household b ON a.household = b.household
    LEFT JOIN location c ON b.location = c.location
    GROUP BY b.household, c.x, c.y, c.zone;"""
    if not os.path.exists(dir.as_posix() + '/Austin-Supply.sqlite'):
        city =  'greenville'
    else:
        city = 'Austin'

    read_bus_sql = """select * from transit_stops;"""
    supply_path = f"""attach database "{dir.as_posix() + '/'+city+'-Supply.sqlite'}" as a;"""
    with sqlite3.connect(dir.as_posix() +'/'+ city+'-Demand.sqlite') as conn:
        conn.cursor().executescript(supply_path);
         # Load households and bus stops data into DataFrames
        households_df = pd.read_sql_query(read_house_sql, conn)
        bus_stops_df = pd.read_sql_query(read_bus_sql, conn)
    bus_stops_coords = bus_stops_df[['X','Y']].values
    households_coords = households_df[['x','y']].values
    tree = BallTree(bus_stops_coords, leaf_size=40)

    batch_size = 1000
    n_jobs = -1  # Use all available CPU cores

    # Create list of tasks
    tasks = [(start, min(start + batch_size, len(households_coords))) for start in range(0, len(households_coords), batch_size)]

    # Print tasks for debugging
    #print("Tasks:", tasks)
   
    p_process_batch_nearest_stops = partial(process_batch_nearest_stops,households_coords=households_coords, tree=tree, households_df=households_df, bus_stops_df=bus_stops_df)

    # Process tasks in parallel
    results = Parallel(n_jobs=n_jobs)(delayed(p_process_batch_nearest_stops)(start, end) for start, end in tasks)
   
    # Flatten the list of results
    flat_results = [item for sublist in results for item in sublist]

    # Convert results to a DataFrame
    results_df = pd.DataFrame(flat_results)
    results_df['miles']=results_df['distance_to_nearest_bus_stop']/1609.34
    

    walking_speed = 2.1 #mph https://www.medicalnewstoday.com/articles/average-walking-speed#average-speed-by-age
    walk_lim = 8 #mins


    results_df['minutes walking']=results_df['miles']/walking_speed*60
    results_df['walkable']=results_df['minutes walking'].apply(lambda x: 0 if x > walk_lim else 1)
   # print(results_df.head(10))
    results_df_sum = results_df.groupby(['walkable', 'vehicles', 'over_60', 'under_60','zone']).agg(count=('income', 'size'),total_income=('income', 'sum')).reset_index()

    dir_name = os.path.split(os.path.split(dir.absolute())[0])[1]
    results_df_sum=results_df_sum.assign(folder=folder)
    results_df_sum.to_csv(dir.as_posix() + '/closest_stops_helper.csv', index=False)
    print(f'Finished stop processing for {dir}.')
    return results_df_sum

def process_batch_nearest_stops(start_idx, end_idx, households_coords, tree, households_df, bus_stops_df, **kwargs):
    #print(f"Processing batch {start_idx} to {end_idx}")
    batch_coords = households_coords[start_idx:end_idx]
    distances, indices = tree.query(batch_coords, k=1)
    batch_results = []
    for i, (dist, idx) in enumerate(zip(distances, indices)):
        household_idx = start_idx + i
        batch_results.append({
            'household_id': households_df.loc[household_idx, 'household'],
            'nearest_bus_stop_id': bus_stops_df.loc[idx[0], 'stop_id'],
            'distance_to_nearest_bus_stop': dist[0],
            'over_60' : households_df.loc[household_idx, 'people_over_60'],
            'under_60': households_df.loc[household_idx, 'people_under_60'],
            'vehicles': households_df.loc[household_idx, 'vehicles'],
            'workers': households_df.loc[household_idx, 'workers'],
            'income': households_df.loc[household_idx, 'income'],
            'zone' : households_df.loc[household_idx, 'zone']
        })
    #print(f"Finished Batch {start_idx} to {end_idx}")
    return batch_results

def process_solo_equiv_fare(iter_dir, demand_db, supply_db, result_db, folder,force_skims, trip_multiplier, config:PostProcessingConfig, **kwargs):
    dir = iter_dir
    supply_db_path = dir.as_posix() + '/'+ supply_db
    result_db_path = dir.as_posix() + '/'+ result_db
    with sqlite3.connect(dir.as_posix() + '/'+ demand_db) as conn:
        if os.path.exists(dir.as_posix() + '/'+'requests_sum_helper.csv') and not force_skims:
            do_skim = False
        else:
            request_sql = f"""select a.tnc_request_id as request,
                                a.request_time,
                                a.discount,
                                a.origin_zone, 
                                a.destination_zone,
                                a.pooled_service, 
                                d.zone,
                                c.vehicles,
                                b.disability,
                                b.race,
                                b.marital_status,
                                c.income,
                                CASE WHEN b.age > 60 THEN 2 WHEN b.age < 18 THEN 0 ELSE 1 END AS age_class
                                from tnc_request a 
                                left join person b on a.person = b.person 
                                left join household c on b.household = c.household 
                                left join location d on c.location = d.location
                                ;"""
            do_skim = True
            conn.cursor().executescript(f"""attach '{supply_db_path}' as a;""")
            conn.cursor().executescript(f"""attach '{result_db_path}' as b;""")
            req_df = pd.read_sql(request_sql, conn).assign(folder=folder)
            fleet_size = pd.read_sql("""select (SELECT count(*) as fleet_size FROM TNC_Statistics) as fleet_size, (select count(*) from tnc_request) as requests;""",conn) * trip_multiplier
        
    if not do_skim and not force_skims:
        t_case_df = pd.read_csv(dir.as_posix() + '/'+'requests_sum_helper.csv')
        
    else:
        print(f"Generating skim request info for {dir}")
        if os.path.exists(dir.as_posix() + '/'+'requests.csv'):
            req_df=pd.read_csv(dir.as_posix() + '/'+'requests.csv')
            print(f"Found request DF for {dir}.")
        else:
            req_df = req_df.sort_values(by=['request_time'])
            timeperiods = get_timeperiods(dir)
            timeperiods.sort()
            last_skim = 0

            file_path = dir.as_posix() + '/'+ 'highway_skim_file.omx'
            omx_file = omx.open_file(file_path, 'r')
            
            mat_taz = omx_file.mapping('taz')
            
            omx_file.close()
            mat_taz = {v: k for k, v in mat_taz.items()}

        
            for index,row in req_df.iterrows():
                request_time= row['request_time']
                origin_zone = row['origin_zone']
                destination_zone = row['destination_zone']
                request_mins = request_time/60.0

                for val in timeperiods:
                    if request_mins <= val:
                        desired_skim = val
                
                            
                if desired_skim != last_skim:
                    omx_file.close()
                    file_path = dir.as_posix() + '/'+'highway_skim_file.omx'
                    try:
                        omx_file = omx.open_file(file_path, 'r')
                        mat_dist_name = f'auto_{desired_skim}_distance'
                        mat_time_name = f'auto_{desired_skim}_time'
                        mat_dist = omx_file[mat_dist_name]
                        mat_time = omx_file[mat_time_name]
                        req_df.loc[index, 'skim_time'] = mat_time[mat_taz[origin_zone],mat_taz[destination_zone]]
                        req_df.loc[index, 'skim_dist'] = mat_dist[mat_taz[origin_zone],mat_taz[destination_zone]]/1609.34
                        last_skim  =desired_skim
                    except AttributeError as e:
                        print(f"{e} omx error in dir {dir}")
                        raise
                    except IndexError as i:
                        print(f"{i} index error in dir {dir} for values origin:{origin_zone}, destination:{destination_zone}, request:{index}")
                else:
                    req_df.loc[index, 'skim_time'] = mat_time[origin_zone,destination_zone]
                    req_df.loc[index, 'skim_dist'] = mat_dist[origin_zone,destination_zone]/1609.34
                    last_skim  =desired_skim

                
            omx_file.close()    
            
            p_min, p_mile, base = get_tnc_pricing(dir, config)
            req_df['solo_equiv_fare_skim'] = req_df['skim_time'] * p_min + req_df['skim_dist'] * p_mile + base
            
            if "heur" in str(dir):
                disc_rate = get_heur_discount(dir)
                req_df.loc[req_df["pooled_service"]==1,'discount_rate'] = disc_rate
                req_df.loc[req_df["pooled_service"]!=1,'discount_rate'] = 0

                req_df.loc[req_df["pooled_service"]==1,'corrected fare']=req_df['solo_equiv_fare_skim']*(1-disc_rate)
                req_df.loc[req_df["pooled_service"]!=1,'corrected fare']=req_df['solo_equiv_fare_skim']
                
                req_df.loc[req_df["pooled_service"]==1,'discount']=req_df['solo_equiv_fare_skim']*(disc_rate)
                req_df.loc[req_df["pooled_service"]!=1,'discount']=0
                #df.to_csv('request_df.csv')
            else:
                req_df.loc[req_df["pooled_service"]==1,'discount_rate']=req_df['discount']
                req_df.loc[req_df["pooled_service"]!=1,'discount_rate']=0

                req_df.loc[req_df["pooled_service"]==1,'corrected fare']=req_df['solo_equiv_fare_skim']*(1-req_df['discount_rate'])
                req_df.loc[req_df["pooled_service"]!=1,'corrected fare']=req_df['solo_equiv_fare_skim']

                req_df.loc[req_df["pooled_service"]==1,'discount']=req_df['solo_equiv_fare_skim']*req_df['discount_rate']
                req_df.loc[req_df["pooled_service"]!=1,'discount']=0
            
            print(f'Finished skims for {dir}.')
            
            #{'total skim solo fare','total fare','total discount','average discount percentage','revenue per vehicle','revenue per request','folder'})
            req_df.to_csv(dir.as_posix() + '/'+'requests.csv', index=False)
            t_case_df = pd.DataFrame({'total skim solo fare':req_df['solo_equiv_fare_skim'].sum()*trip_multiplier ,
                            'total fare':req_df['corrected fare'].sum()*trip_multiplier,
                            'total discount':req_df['discount'].sum()*trip_multiplier,
                            'average discount percentage':req_df['discount_rate'].mean() ,
                            'revenue per vehicle':req_df['corrected fare'].sum()*trip_multiplier/fleet_size['fleet_size'],
                            'revenue per request':req_df['corrected fare'].sum()*trip_multiplier/fleet_size['requests'],'folder':folder})
            t_case_df.to_csv(dir.as_posix() + '/'+'requests_sum_helper.csv', index=False)

    return t_case_df
        
def process_elder_request_agg(iter_dir, trip_multiplier, supply_db, result_db, demand_db,folder, config:PostProcessingConfig, **kwargs):
        dir = iter_dir
        req_df= pd.read_csv(dir.as_posix() + '/'+'requests.csv')
        supply_db_path = dir.as_posix() + '/'+ supply_db
        result_db_path = dir.as_posix() + '/'+ result_db

        if os.path.exists(dir.as_posix() + '/'+'tnc_skim_demo.csv') and not config.force_skims:
            t_case_demo_df = pd.read_csv(dir.as_posix() + '/'+'tnc_skim_demo.csv')
        else:
            with sqlite3.connect(dir.as_posix() + '/'+ demand_db) as conn:
                conn.cursor().executescript(f"""attach '{supply_db_path}' as a;""")
                conn.cursor().executescript(f"""attach '{result_db_path}' as b;""")
                fleet_size = pd.read_sql("""select (SELECT count(*) as fleet_size FROM TNC_Statistics) as fleet_size, (select count(*) from tnc_request) as requests;""",conn) * trip_multiplier
            
            groupings = ['zone', 'vehicles', 'disability', 'race', 'marital_status', 'income', 'age_class']

            # Group the dataframe by the specified columns and perform the aggregations
            t_case_demo_df = req_df.groupby(groupings).agg({
                'solo_equiv_fare_skim': 'sum',
                'corrected fare': 'sum',
                'discount': 'sum',
                'discount_rate': 'sum',
                'request': 'count'
            }).reset_index()

            # Calculate the additional columns
            t_case_demo_df['total skim solo fare'] = t_case_demo_df['solo_equiv_fare_skim']*trip_multiplier
            t_case_demo_df['total fare'] = t_case_demo_df['corrected fare']*trip_multiplier
            t_case_demo_df['total discount'] = t_case_demo_df['discount']*trip_multiplier
            t_case_demo_df['total discount percentage'] = t_case_demo_df['discount_rate']*trip_multiplier
            t_case_demo_df['folder'] = folder
            t_case_demo_df['request_count'] = t_case_demo_df['request']*trip_multiplier

            # Drop the intermediate aggregation columns if not needed
            t_case_demo_df.drop(columns=['solo_equiv_fare_skim', 'corrected fare', 'discount', 'discount_rate','request'], inplace=True)
        
            
        
            t_case_demo_df.to_csv(dir.as_posix() + '/'+'tnc_skim_demo.csv', index=False)
        return t_case_demo_df

def process_tnc_stat_summary(iter_dir, demand_db,folder, config:PostProcessingConfig, **kwargs):
    
    with  sqlite3.connect(iter_dir.as_posix()+ '/'+demand_db) as conn:
        df = pd.read_sql("select * from tnc_stat_summary_helper",conn)
    #served_requests = df.dropna(subset=['assigned_vehicle'])
    df['assigned_vehicle'] = df['assigned_vehicle'].apply(lambda x: 0 if pd.isna(x) else 1)
    scale_factor = get_scale_factor(iter_dir,config)
    assigned_vehicles = df['assigned_vehicle'].sum()*scale_factor
    requests = df['assigned_vehicle'].count()*scale_factor
    df = df.drop(df[df['assigned_vehicle']==0].index)
    columns_to_sum = [
        'wait_min',
        'ttime',
        'discount',
        'fare',
        'pooled',
        'eVMT_perc',
        'occupied_VMT',
        'VMT',
        'mileage_AVO',
        'mileage_rAVO',
        'trip_AVO',
        'trip_rAVO',
        'operating_cost',
        'revenue'
        ]
    summary_df = df[columns_to_sum].agg(['sum','mean', 'std', 'count']).transpose().reset_index()
    summary_df= pd.concat([summary_df,pd.DataFrame({'index': 'assigned_requests','sum':assigned_vehicles},index=[0])])
    summary_df= pd.concat([summary_df,pd.DataFrame({'index': 'requests','sum':requests},index=[0])])
    # Rename columns for clarity
    summary_df.columns = ['Metric', 'Sum', 'Mean', 'StdDev', 'SampleSize']
    summary_df = summary_df.assign(folder=folder).reset_index()
    summary_df=summary_df.drop(columns=['index'])
    summary_df.to_csv(iter_dir.as_posix() + '/tnc_stat_summary_helper.csv')
    return summary_df


