import os
import sqlite3
import pandas as pd
import numpy as np
#import .utils
from .utils import get_timeperiods, get_tnc_pricing, get_heur_discount, get_repositioning_interval, get_strategies
import openmatrix as omx
from CU_POLARIS_Postprocessor.config import PostProcessingConfig
from CU_POLARIS_Postprocessor.utils import get_scale_factor, get_fleet_size
from functools import partial
import shutil

def process_nearest_stops(iter_dir,supply_db,demand_db, folder, config:PostProcessingConfig, **kwargs):
    dir=iter_dir
    
    if os.path.exists(dir.as_posix() + '/closest_stops_helper.csv'):
        results_df_sum = pd.read_csv(dir.as_posix() + '/closest_stops_helper.csv')
        if len(results_df_sum) > 1:
            return results_df_sum
        
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
    

    
    read_bus_sql = """select * from transit_stops;"""
    supply_path = f"""attach database "{supply_db}" as a;"""
    with sqlite3.connect(demand_db) as conn:
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
    
    with sqlite3.connect(demand_db) as conn:
        if os.path.exists(dir.as_posix() + '/'+'requests_sum_helper.csv') and not force_skims:
            t_case_df = pd.read_csv(dir.as_posix() + '/'+'requests_sum_helper.csv')
            return t_case_df
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
                                where a.service_type <> 99
                                ;"""
            do_skim = True
            conn.cursor().executescript(f"""attach '{supply_db}' as a;""")
            conn.cursor().executescript(f"""attach '{result_db}' as b;""")
            req_df = pd.read_sql(request_sql, conn).assign(folder=folder)
            fleet_size = pd.read_sql("""select (SELECT count(*) as fleet_size FROM TNC_Statistics) as fleet_size, (select count(*) from tnc_request where service_type <> 99) as requests;""",conn) * trip_multiplier
        
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
            
            assignment_strategy, _ = get_strategies(dir,config)
            if assignment_strategy == "default":
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
        
        
        if os.path.exists(dir.as_posix() + '/'+'tnc_skim_demo.csv') and not config.force_skims:
            t_case_demo_df = pd.read_csv(dir.as_posix() + '/'+'tnc_skim_demo.csv')
        else:
            with sqlite3.connect(demand_db) as conn:
                conn.cursor().executescript(f"""attach '{supply_db}' as a;""")
                conn.cursor().executescript(f"""attach '{result_db}' as b;""")
                fleet_size = pd.read_sql("""select (SELECT count(*) as fleet_size FROM TNC_Statistics) as fleet_size, (select count(*) from tnc_request where service_type <> 99) as requests;""",conn) * trip_multiplier
            
            req_df= pd.read_csv(dir.as_posix() + '/'+'requests.csv')
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
    # iter_dir.as_posix() + '/tnc_stat_summary_helper.csv'

    dir = iter_dir
    
        
    if os.path.exists(dir.as_posix() + '/tnc_stat_summary_helper.csv') and not config.force_skims:
        summary_df = pd.read_csv(dir.as_posix() + '/tnc_stat_summary_helper.csv')
    else:

        with  sqlite3.connect(demand_db) as conn:
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

def process_demo_financial_case_data(iter_dir, demand_db, result_db,folder, request_file_name, trip_multiplier, config:PostProcessingConfig, **kwargs):
    dir = iter_dir
    if os.path.exists(dir.as_posix() +"/request_case_summary.csv"):
         req_case_sum = pd.read_csv(dir.as_posix() +"/request_case_summary.csv")
         return req_case_sum
    
    
    ride_min_min = 5
    per_min_min=1.4
    per_mile_min=0.51

    uber_take=0.21

    km_mile = 0.621371

    fare_pub_low= 2
    fare_pub_high = 4
    op_pub_van=3.54/km_mile
    op_pub_minibus=1.96/km_mile

    op_cost_saev_p = 0.58/km_mile
    op_cost_saev_np=0.48/km_mile
    req_case_sum = pd.DataFrame()


    with sqlite3.connect(result_db) as conn:
        conn.cursor().executescript(f"""attach database '{demand_db}' as a;""");
        requests = pd.read_sql("""select (SELECT count(*) as fleet_size FROM TNC_Statistics) as fleet_size, (select count(*) from tnc_request where service_type <> 99) as requests;""",conn) * trip_multiplier
        rev_vht_vmt = pd.read_sql(f"""select sum(end-start)/60 as revenue_minutes_traveled, sum(travel_distance)/1609.34 as revenue_miles_traveled from tnc_trip a left join tnc_request b on a.request = b.tnc_request_id  where case when b.service_type = 99 then 0 else a.passengers end >0""",conn)*trip_multiplier
        
    fleet_size = get_fleet_size(iter_dir, config)

        
    dir_name = os.path.split(os.path.split(iter_dir.absolute())[0])[1]
    iteration_num = iter_dir.name.split('_')[-1]
    req_df = pd.read_csv(iter_dir.as_posix()+'/'+request_file_name).assign(folder=folder)
    req_df['pay_1_a_calc'] = req_df['skim_dist']*per_mile_min+req_df['skim_time']*per_min_min
    req_df['pay_1_a_act']=req_df['pay_1_a_calc'].apply(lambda x: x if x >= 5 else 5)
    #get the fare for case 1 from the request summary row

    #1_b comes straight from the trips but need to count requests

    req_df['fare_2_low']=2
    req_df['fare_2_high']=4
    req_df['op_2_van']=req_df['skim_dist']*op_pub_van
    req_df['op_2_minibus']=req_df['skim_dist']*op_pub_minibus
    
    req_df.loc[req_df["pooled_service"]==1,'op3']=req_df['skim_dist']*op_cost_saev_p
    req_df.loc[req_df["pooled_service"]!=1,'op3']=req_df['skim_dist']*op_cost_saev_np
    iteration_num = iter_dir.name.split('_')[-1]


    req_sum_df = req_df.groupby(['age_class','origin_zone']).apply(lambda x: pd.Series({
    'total pay 1_a': x['pay_1_a_act'].sum() * trip_multiplier,
    '1_a_cnt_req_under_5': (x['pay_1_a_calc'] < 5).sum() * trip_multiplier,
    'total_fare_2_low': x['fare_2_low'].sum() * trip_multiplier,
    'total_fare_2_high': x['fare_2_high'].sum() * trip_multiplier,
    'total_operating_cost_2_minibus': x['op_2_minibus'].sum() * trip_multiplier,
    'total_operating_cost_2_van': x['op_2_van'].sum() * trip_multiplier,
    'total_operating_cost_3': x['op3'].sum() * trip_multiplier,
    'total_fare': x['corrected fare'].sum() * trip_multiplier,
    'requests': requests['requests'].iloc[0],  # Extract scalar value
    'fleet_size': fleet_size,  # Extract scalar value
    'revenue per vehicle': x['corrected fare'].sum() * trip_multiplier / fleet_size,
    'revenue per request': x['corrected fare'].sum() * trip_multiplier / requests['requests'].iloc[0],
    'revenue_minutes_traveled': rev_vht_vmt['revenue_minutes_traveled'].iloc[0],  # Extract scalar value
    'revenue_miles_traveled': rev_vht_vmt['revenue_miles_traveled'].iloc[0],  # Extract scalar value
    'folder': folder,
    })).reset_index()


    req_case_sum = pd.concat([req_case_sum,req_sum_df])
    req_case_sum['uber take']=req_case_sum['total_fare']*uber_take
    req_case_sum.to_csv(dir.as_posix() +"/request_case_summary.csv",index=False)
    return req_case_sum

def process_tnc_repositioning_success_rate(iter_dir, demand_db, supply_db, result_db, folder, trip_multiplier, config, **kwargs):
    #get the trips df
        #get the trips df
    repo_interval = 900
    csv_path = os.path.join(iter_dir, f"{folder}_tnc_repositioning_success_rate.csv")
    if os.path.exists(csv_path) and not (config.reset_csvs):
        return pd.read_csv(csv_path)

    trip_query = f"""select 
                    a.vehicle, 
                    a.start,
                    a.end,
                    b.origin_zone,
                    b.destination_zone,
                    a.init_status,
                    a.passengers,
                    a.skim_travel_time/60 as skim_travel_time,
                    a.routed_travel_time/60 as routed_travel_time,
                    a.request,
                    a.travel_distance/1609.34 as travel_distance,  -- Convert meters to miles
                    cast (a.start/3600 as int) as hour,
                    cast (a.start/{repo_interval} as int) as repositioning_period,
                    a.init_status,
                    b.service_type as service_type,
                    a.tour
                    from 
                    tnc_trip a 
                    left join tnc_request b on a.request = b.tnc_request_id;"""

    request_query = f"""select *
                        from (
                        (select sum(1-pooled_service)*1.0/(count(*)*1.0) as solo_perc from tnc_request where service_type <> 99)  as solo_perc,
                        (select sum(pooled_service)*1.0/(count(*)*1.0) as pooled_perc from tnc_request where service_type <> 99) as pooled_perc,
                        (SELECT SUM(fare) * {trip_multiplier} as total_fare FROM tnc_request) as total_fare,
                        (SELECT SUM(fare)/(count(*) * 1.0) as average_fare FROM tnc_request where service_type <> 99) as average_fare,
                        (SELECT SUM(discount) * {trip_multiplier} as total_discount FROM tnc_request) as total_discount,
                        (SELECT SUM(discount)/(count(*) * 1.0) as average_discount FROM tnc_request where service_type <> 99) as average_discount,
                        (SELECT count(*) * {trip_multiplier} as requests FROM tnc_request where service_type <> 99) as requests,
                        (SELECT count(*) * {trip_multiplier} as repo_requests FROM tnc_request where service_type = 99 and assigned_vehicle is not null) as repo_requests,
                        (SELECT count(*) * {trip_multiplier} as pooled_requests FROM tnc_request where service_type <> 99 and pooled_service = 1 and assigned_vehicle is not null) as pooled_requests,
                        (SELECT count(*) * {trip_multiplier} as solo_requests FROM tnc_request where service_type <> 99 and pooled_service = 0 and assigned_vehicle is not null) as solo_requests,
                        (SELECT count(*) * {trip_multiplier} as rejected_requests FROM tnc_request where service_type <> 99 and assigned_vehicle is null) as rejected_requests,
                        (SELECT avg(pickup_time - request_time)/60 as average_wait_time_min FROM tnc_request where service_type <> 99) as average_wait_time_min,
                        (SELECT CAST(count(case when assigned_vehicle is null then 1 end) AS FLOAT) / CAST(count(*) AS FLOAT) as rejection_rate FROM tnc_request where service_type <> 99) AS rejection_rate,
                        (SELECT sum(discount)/sum(fare) as average_discount_rate FROM tnc_request) as average_discount_rate,
                        (SELECT sum(dropoff_time - pickup_time)/60 /(count(*)*1.0) as average_person_ttime_min FROM tnc_request where service_type <> 99) as average_person_ttime_min,
                        (SELECT sum(dropoff_time - pickup_time)/60 /(count(*)*1.0) as average_repo_ttime_min FROM tnc_request where service_type = 99) as average_repo_ttime_min,
                        (SELECT sum(((dropoff_time - pickup_time) - estimated_od_travel_time)/60)/(count(*) * 1.0) as average_pooled_delay_time_min FROM tnc_request where service_type <> 99 and pooled_service = 1) as average_pooled_delay_time_min,
                        (SELECT sum(((dropoff_time - pickup_time) - estimated_od_travel_time)/60)/(count(*) * 1.0) as average_solo_delay_time_min FROM tnc_request where service_type <> 99 and pooled_service = 0) as average_solo_delay_time_min);"""



    def split_on_zero_pax(group):
        # We assume the first row is deadheading with 0 passengers — keep in same sub-tour
        sub_tour = []
        sub_tour_id = 0
        for i, row in enumerate(group.itertuples()):
            # If passengers == 0 and not first segment, increment sub-tour ID
            if i != 0 and row.passengers == 0:
                sub_tour_id += 1
            sub_tour.append(sub_tour_id)
        group['sub_tour'] = sub_tour
        return group







    with sqlite3.connect(demand_db) as conn:
        conn.cursor().executescript(f"""attach database '{supply_db}' as a;""");
        trips_df = pd.read_sql(trip_query, conn)
        requests_df = pd.read_sql(request_query, conn)

    #correcting tours
    trips_df = trips_df.sort_values(['vehicle', 'tour', 'start']).reset_index(drop=True)
    trips_df = trips_df.groupby(['vehicle', 'tour'], group_keys=False).apply(split_on_zero_pax)

    # Optional: Create a final unique tour ID
    trips_df['final_tour_id'] = (trips_df['vehicle'].astype(str) + '_tour_' + trips_df['tour'].astype(str) + '_sub_' + trips_df['sub_tour'].astype(str))

    
    #aggregate to tours
    tour_df = trips_df.groupby(['vehicle', 'tour', 'sub_tour'], as_index=False).agg({
        'start': 'min',
        'end': 'max',
        'passengers': 'max',
        'skim_travel_time': 'sum',
        'routed_travel_time': 'sum',
        'request': lambda x: list(x),
        'service_type': 'first',
        'hour': 'first',
        'origin_zone': 'first',
        'destination_zone': 'last',
        'travel_distance': 'sum',
        'init_status':'first'
    }).copy()

    tour_df = tour_df.rename(columns={
        'start': 'tour_start',
        'end': 'tour_end',
        'skim_travel_time': 'total_skim_travel_time',
        'routed_travel_time': 'total_routed_travel_time',
        'request': 'requests',
        'passengers': 'max_passengers',
        'travel_distance': 'total_travel_distance'
    })

    tour_df["requests_served"] = tour_df["requests"].apply(lambda x: len(x) if isinstance(x, list) else 0)
    tour_df["pooled_tour"] = tour_df["requests_served"] > 1


    # Ensure trips are sorted
    trips_df = trips_df.sort_values(by=["vehicle", "start"])

    # Separate repositioning and service tours
    if (trips_df["service_type"] == 99).any():
        repositioning = trips_df[(trips_df["service_type"] == 99) & (trips_df["init_status"] == -2)].copy()
        service_trips = trips_df[trips_df["service_type"] != 99].copy()
    else:
        repositioning = trips_df[(trips_df["init_status"] == -3)].copy()
        service_trips = trips_df[trips_df["init_status"] != -3].copy()


    #if there are no repositioning trips, output a dummy dataframe with 0 in each hour row with the same columns as repositioning
    if repositioning.empty:
        hour_range = pd.date_range(start=trips_df["start"].min().floor("H"), end=trips_df["end"].max().ceil("H"), freq="H")
        results = pd.DataFrame({
            "hour": hour_range,
            "num_repositioned": 0,
            "num_successful": 0,
            "success_rate": 0
        })

    hour_grouped = repositioning.groupby("hour")

    results = []

    for hour, group in hour_grouped:
        success_count = 0
        total_count = 0

        for _, rep_row in group.iterrows():
            vehicle_id = rep_row["vehicle"]
            rep_end_time = rep_row["start"]
            dest_zone = rep_row["destination_zone"]

            # Get next trip by vehicle within 1 hour
            future_trips = service_trips[
                (service_trips["vehicle"] == vehicle_id) &
                (service_trips["start"] > rep_end_time) &
                (service_trips["start"] <= rep_end_time + 3600)  # Adjusted to use repositioning interval
            ].sort_values(by="start")

            total_count += 1
            if not future_trips.empty:
                next_trip = future_trips.iloc[0]
                if next_trip["origin_zone"] == dest_zone:
                    success_count += 1

        success_rate = success_count / total_count if total_count > 0 else 0
        results.append({
            "hour": hour,
            "num_repositioned": total_count,
            "num_successful": success_count,
            "success_rate": success_rate
        })

    # Create a summary dataframe
    summary_df = pd.DataFrame(results)
    summary_df["folder"] = folder
    summary_df.sort_values(by="hour").reset_index(drop=True)

    #agg into daily summary
    daily_summary_df = summary_df.groupby(["folder"]).agg({
        "num_repositioned": "sum",
        "num_successful": "sum"
    }).reset_index()
    #allow for zero division
    daily_summary_df["success_rate"] = daily_summary_df["num_successful"] / daily_summary_df["num_repositioned"].replace(0, np.nan)  # Avoid division by zero
    daily_summary_df = daily_summary_df.fillna(0)  # Replace NaN with
    #add columns for average routed travel time, average travel distance, average skim travel time, total routed travel time, average skim travel time and travel distance, empty vehicle travel time and distance total (passengers = 0) 
    
    if (trips_df["service_type"] == 99).any():
        daily_summary_df["average_routed_ttime_repo"] = tour_df[tour_df["service_type"] == 99]["total_routed_travel_time"].mean()
        daily_summary_df["average_skim_ttime_repo"] = tour_df[tour_df["service_type"] == 99]["total_skim_travel_time"].mean()
        daily_summary_df["average_travel_distance_repo"] = tour_df[tour_df["service_type"] == 99]["total_travel_distance"].mean()
    else:
        daily_summary_df["average_routed_ttime_repo"] = tour_df[tour_df["init_status"] == -3]["total_routed_travel_time"].mean()
        daily_summary_df["average_skim_ttime_repo"] = tour_df[tour_df["init_status"] == -3]["total_skim_travel_time"].mean()
        daily_summary_df["average_travel_distance_repo"] = tour_df[tour_df["init_status"] == -3]["total_travel_distance"].mean()

    daily_summary_df["average_routed_ttime_pooled"] = tour_df[tour_df["pooled_tour"] == True]["total_routed_travel_time"].mean()
    daily_summary_df["average_skim_ttime_pooled"] = tour_df[tour_df["pooled_tour"] == True]["total_skim_travel_time"].mean()
    daily_summary_df["average_travel_distance_pooled"] = tour_df[tour_df["pooled_tour"] == True]["total_travel_distance"].mean()


    if (trips_df["service_type"] == 99).any():
        daily_summary_df['average_routed_ttime_solo'] = tour_df[(tour_df["pooled_tour"] == False) & (tour_df["service_type"] != 99)]["total_routed_travel_time"].mean()
        daily_summary_df['average_skim_ttime_solo'] = tour_df[(tour_df["pooled_tour"] == False) & (tour_df["service_type"] != 99)]["total_skim_travel_time"].mean()
        daily_summary_df['average_travel_distance_solo'] = tour_df[(tour_df["pooled_tour"] == False) & (tour_df["service_type"] != 99)]["total_travel_distance"].mean()
    else:
        daily_summary_df['average_routed_ttime_solo'] = tour_df[(tour_df["pooled_tour"] == False) & (tour_df["init_status"] != -3)]["total_routed_travel_time"].mean()
        daily_summary_df['average_skim_ttime_solo'] = tour_df[(tour_df["pooled_tour"] == False) & (tour_df["init_status"] != -3)]["total_skim_travel_time"].mean()
        daily_summary_df['average_travel_distance_solo'] = tour_df[(tour_df["pooled_tour"] == False) & (tour_df["init_status"] != -3)]["total_travel_distance"].mean()


    daily_summary_df["total_travel_time"] = tour_df["total_routed_travel_time"].sum()
    daily_summary_df["total_travel_distance"] = tour_df["total_travel_distance"].sum()
    daily_summary_df["total_empty_vehicle_travel_distance"] = tour_df[tour_df["max_passengers"] == 0]["total_travel_distance"].sum()
    daily_summary_df["pooled_tour_count"] = tour_df[tour_df["pooled_tour"] == True].shape[0]

    #avo = sum(passengers * mileage) over sum mileage
    daily_summary_df["average_vehicle_occupancy"] = (trips_df["passengers"] * trips_df["travel_distance"]).sum() / trips_df["travel_distance"].sum()
    #ravo = sum(passengers * mileage) over sum mileage where only trips with at lease one passenger are considered
    daily_summary_df["average_revenue_vehicle_occupancy"] = (trips_df[trips_df["passengers"] > 0]["passengers"] * trips_df[trips_df["passengers"] > 0]["travel_distance"]).sum() / trips_df[trips_df["passengers"] > 0]["travel_distance"].sum()

    #average pickup segment length      
    if (trips_df["service_type"] == 99).any():
        daily_summary_df["average_pickup_segment_distance"] = trips_df[(trips_df["passengers"] < 1) & (trips_df["service_type"] != 99)]["travel_distance"].mean()
    else:
        daily_summary_df["average_pickup_segment_distance"] = trips_df[(trips_df["passengers"] < 1) & (tour_df["init_status"] != -3)]["travel_distance"].mean()


    requests_df["folder"] = folder
    #join requests_df with daily_summary_df
    daily_summary_df = daily_summary_df.join(requests_df.set_index("folder"), on="folder", how="left")


    daily_summary_df.to_csv(csv_path, index=False)
   
    # Optional: sort by hour
    return daily_summary_df
    
    