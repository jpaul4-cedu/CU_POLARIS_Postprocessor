from .config import PostProcessingConfig
from pathlib import Path

def get_sql_create(supply_db=None,trip_multiplier=None,result_db=None,config:PostProcessingConfig=None,dir:Path=None):
    if not config is None:
        if dir is None:
            raise ValueError("If config is provided, dir is required.")
        db_dir = config.folder_db_map[dir]
        supply_db = db_dir["supply"]
        demand_db = db_dir["demand"]
        result_db = db_dir["result"]
        trip_multiplier = db_dir["trip_multiplier"]
    
    
    queries = {
    "attach": f"""
        ATTACH DATABASE "{result_db}" as a;
        ATTACH DATABASE "{supply_db}" as b;
    """,
    "transit_trip_max_load_helper": f"""
        create table if not exists transit_trip_max_load_helper as 
        select a.object_id as trip_id, b.pattern_id, c.route_id, 
        max(a.value_seated_load * "{trip_multiplier}" + a.value_standing_load * "{trip_multiplier}") as max_load, 
        sum((a.value_seated_load * "{trip_multiplier}" + a.value_standing_load* "{trip_multiplier}")*value_length)/sum(value_length) as AVO,
        sum(value_length) as trip_length 
        from transit_vehicle_links a 
        left join transit_trips b on a.object_id = b.trip_id 
        left join transit_patterns c on b.pattern_id = c.pattern_id 
        group by trip_id order by max_load;
    """,
    "transit_trip_max_load": f"""
        create table if not exists transit_trip_max_load as select * from (
        (select count(*) as empty_trips from transit_trip_max_load_helper where max_load = 0),
        (select count(*) as tnc_replaceable_routes from (select max(max_load) as max_load, route_id from transit_trip_max_load_helper group by route_id) where max_load <=4));
    """,
    "mode_Distribution_ADULT_Counts": f"""
        CREATE TABLE IF NOT EXISTS mode_Distribution_ADULT_Counts As
        select mode, CASE WHEN person.age > 60 THEN 2 WHEN person.age < 18 THEN 0 ELSE 1 END AS age_class,
        {trip_multiplier} *(sum(case when trip.destination = person.work_location_id then 1 else 0 end)) as 'HBW',
        {trip_multiplier} *(sum(case when trip.origin = household.location and trip.destination <> person.work_location_id then 1 else 0 end)) as 'HBO',
        {trip_multiplier} *(sum(case when trip.origin <> household.location and trip.destination <> household.location and trip.destination <> person.work_location_id then 1 else 0 end)) as 'NHB',
        {trip_multiplier} *(count(*)) as total
        from trip, person, household 
        left join location on household.location = location.location
        where trip.person = person.person and person.household = household.household and trip."end" - trip."start" > 2 and (has_artificial_trip = 0 or mode in (7,8))
        group by mode, age_class;
    """,
    "mode_Distribution_ADULT_Distance": f"""
        CREATE TABLE IF NOT EXISTS mode_Distribution_ADULT_Distance As
        SELECT
        mode, CASE WHEN person.age > 60 THEN 2 WHEN person.age < 18 THEN 0 ELSE 1 END AS age_class,
        {trip_multiplier} * sum(travel_distance/1609.34 * (trip.origin <> household.location AND trip.destination <> household.location AND trip.destination <> person.work_location_id)) AS 'NHB',
        {trip_multiplier} * sum(travel_distance/1609.34 * (trip.destination = person.work_location_id)) AS 'HBW',
        {trip_multiplier} * sum(travel_distance/1609.34 * (trip.origin == household.location AND trip.destination <> person.work_location_id)) AS 'HBO',
        {trip_multiplier} * sum(travel_distance/1609.34) AS total
        FROM trip, person, household
        WHERE trip.person = person.person AND person.household = household.household AND trip."end" - trip."start" > 2 and (has_artificial_trip = 0 or mode in (7,8)) 
        GROUP BY mode, age_class;
    """,
    "bus_avo": f"""
        CREATE TABLE if not exists bus_avo AS
        SELECT * from (
        (select sum(value_length * (value_seated_load * {trip_multiplier} + value_standing_load * {trip_multiplier}))/(sum(value_length)) as mileage_avo from transit_vehicle_links),
        (select sum(value_seated_load + value_standing_load)/(count(*)*1.0) as trip_avo from transit_vehicle_links),
        (select count(*) as cnt_mass, sum(value_length/1609.34) as dist_mass from transit_vehicle_links where (value_seated_load + value_standing_load) * {trip_multiplier} > 4),
        (select count(*) as cnt_pool, sum(value_length/1609.34) as dist_pool from transit_vehicle_links where (value_seated_load + value_standing_load) * {trip_multiplier} > 1 and (value_seated_load + value_standing_load) * {trip_multiplier} <= 4),
        (select count(*) as cnt_solo, sum(value_length/1609.34) as dist_solo from transit_vehicle_links where (value_seated_load + value_standing_load) * {trip_multiplier} = 1),
        (select count(*) as cnt_empt, sum(value_length/1609.34) as dist_empt from transit_vehicle_links where (value_seated_load + value_standing_load) * {trip_multiplier} < 1));
    """,
    "pr_avo": f"""
        CREATE TABLE if not exists pr_avo AS
        select * from (
        (select count(*) * {trip_multiplier} as cnt_pool, sum(travel_distance/1609.34) * {trip_multiplier} as dist_pool from (select a.*, b.service_type from tnc_trip a left join tnc_request b on a.request = b.tnc_request_id) where (case when service_type = 99 then 0 else passengers end) > 1),
        (select count(*) * {trip_multiplier} as cnt_solo, sum(travel_distance/1609.34) * {trip_multiplier} as dist_solo from (select a.*, b.service_type from tnc_trip a left join tnc_request b on a.request = b.tnc_request_id) where (case when service_type = 99 then 0 else passengers end) = 1),
        (select count(*) * {trip_multiplier} as cnt_empt, sum(travel_distance/1609.34) * {trip_multiplier} as dist_empt from (select a.*, b.service_type from tnc_trip a left join tnc_request b on a.request = b.tnc_request_id) where (case when service_type = 99 then 0 else passengers end) < 1));
    """,
    "fare_sensitivity_results": f"""
        CREATE TABLE if not exists fare_sensitivity_results AS SELECT *, t3.evmt * 1.0 / t4.sav_vmt as perc_evmt, t4.sav_vmt * 1.0 / t2.requests_served as vmt_per_r, t6.sav_vht * 60.0 / t2.requests_served as vht_per_r, t2b.repo_requests_served FROM 
        (SELECT count(*) * {trip_multiplier} as fleet_size, avg(tot_dropoffs) as avg_trips FROM TNC_Statistics) as t1,
        (SELECT avg(pickup_time - request_time)/60 as wait_min, avg(dropoff_time - request_time)/60 as ttime_min, count(*) * {trip_multiplier} as requests_served FROM TNC_Request where assigned_vehicle is not null and service_type <> 99) as t2,
        (SELECT count(*) * {trip_multiplier} as repo_requests_served from TNC_Request where assigned_vehicle is not null and service_type = 99) as t2b,
        (SELECT sum(a.travel_distance)/1609.34 * {trip_multiplier} as evmt FROM  TNC_Trip a left join tnc_request b on a.request = b.tnc_request_id where (case when b.service_type = 99 then 0 else a.passengers end) = 0) as t3,
        (SELECT sum(travel_distance)/1609.34 * {trip_multiplier} as sav_vmt FROM TNC_Trip) as t4,
        (SELECT sum(a.travel_distance * (case when b.service_type = 99 then 0 else a.passengers end)) * 1.0 / sum(travel_distance ) as AVO FROM TNC_Trip a left join tnc_request b on a.request = b.tnc_request_id) as t5,
        (SELECT sum(a.travel_distance * (case when b.service_type = 99 then 0 else a.passengers end)) * 1.0 / sum(travel_distance ) as rAVO FROM TNC_Trip a left join tnc_request b on a.request = b.tnc_request_id where (case when b.service_type = 99 then 0 else a.passengers end) > 0) as t8,
        (SELECT sum(end - start)/3600 * {trip_multiplier} as sav_vht FROM TNC_Trip) as t6,
        (SELECT count(distinct TNC_Request_id) * {trip_multiplier} as unserved_requests FROM TNC_Request WHERE assigned_vehicle is null and service_type <>99) as t7,
        (SELECT sum(travel_distance)/1609.34 * {trip_multiplier} as sov_vmt, sum(end-start)/3600 * {trip_multiplier} as sov_vht FROM trip where mode = 0) as t8,
        (select ((select sum(case when b.service_type = 99 then 0 else a.passengers end) from tnc_trip a left join tnc_request b on a.request = b.tnc_request_id) * 1.0 / (select count(*) from tnc_request)) as trip_avo) as t9,
        (select ((select sum(case when b.service_type = 99 then 0 else a.passengers end) from tnc_trip a left join tnc_request b on a.request = b.tnc_request_id where passengers > 0) * 1.0 / (select count(distinct request) from tnc_trip)) as trip_ravo) as t10;
    """,
    "mode_Distribution_ADULT": f"""
        CREATE TABLE IF NOT EXISTS mode_Distribution_ADULT As
        select mode, 
        {trip_multiplier}*(sum(case when trip.destination = person.work_location_id then 1 else 0 end)) as 'HBW',
        {trip_multiplier}*(sum(case when trip.origin = household.location and trip.destination <> person.work_location_id then 1 else 0 end)) as 'HBO',
        {trip_multiplier}*(sum(case when trip.origin <> household.location and trip.destination <> household.location and trip.destination <> person.work_location_id then 1 else 0 end)) as 'NHB',
        {trip_multiplier}*(count(*)) as total,
        location.zone
        from trip, person, household 
        left join location on household.location = location.location
        where trip.person = person.person and person.household = household.household and person.age > 16 and trip."end" - trip."start" > 2
        group by mode, zone;
    """,
    "distance_tnc_dist": f"""CREATE TABLE if not exists distance_tnc_dist AS
        SELECT cast(distance*2 as int)/2 as binned_dist, count(*) * {trip_multiplier} as num_trips, service_type FROM TNC_Request WHERE assigned_vehicle is not null GROUP by binned_dist;
        """,
    "fare_sensitivity_results_zonal":f"""
        CREATE TABLE if not exists fare_sensitivity_results_zonal AS select a.zone, a.area_type, a.wait_min, a.ttime_min, a.requests_served, unserved_requests, fare 
        from (SELECT zone.zone, zone.area_type, avg(pickup_time - request_time)/60 as wait_min, avg(dropoff_time - request_time)/60 as ttime_min, count(*) * {trip_multiplier} as requests_served, sum(fare) as fare
        FROM TNC_Request left join location on tnc_request.origin_location = location.location left join zone on location.zone = zone.zone where assigned_vehicle is not null and service_type <> 99
        group by zone.zone) as a left join 
        (SELECT zone.zone, zone.area_type, count(distinct TNC_Request_id) * {trip_multiplier} as unserved_requests FROM TNC_Request left join location on tnc_request.origin_location = location.location left join zone on location.zone = zone.zone WHERE assigned_vehicle is null and service_type <> 99 group by zone.zone) as b 
        on a.zone = b.zone;""",
    "fare_sensitivity_demograpic_tnc_stats":f"""
        CREATE TABLE if not exists fare_sensitivity_demograpic_tnc_stats AS
        SELECT COUNT(*) * {trip_multiplier} AS REQUESTS, AVG(FARE) AS AVG_FARE, AVG(DISTANCE)/1609.34 as AVG_DIST, AVG(PICKUP_TIME-REQUEST_TIME)/60 AS AVG_WAIT_MIN, AVG(DROPOFF_TIME-REQUEST_TIME)/60 AVG_TTIME_MIN, AVG(HOUSEHOLD.VEHICLES) as VO, ZONE, HOUSEHOLD.PERSONS, HOUSEHOLD.WORKERS, PERSON.EDUCATION, PERSON.GENDER, PERSON.RACE,
        CASE
        WHEN HOUSEHOLD.INCOME >=0 AND HOUSEHOLD.INCOME <10000 THEN 0
        WHEN HOUSEHOLD.INCOME >=10000 AND HOUSEHOLD.INCOME <20000 THEN 1
        WHEN HOUSEHOLD.INCOME >=20000 AND HOUSEHOLD.INCOME <30000 THEN 2
        WHEN HOUSEHOLD.INCOME >=30000 AND HOUSEHOLD.INCOME <40000 THEN 3
        WHEN HOUSEHOLD.INCOME >=40000 AND HOUSEHOLD.INCOME <50000 THEN 4
        WHEN HOUSEHOLD.INCOME >=50000 AND HOUSEHOLD.INCOME <60000 THEN 5
        WHEN HOUSEHOLD.INCOME >=60000 AND HOUSEHOLD.INCOME <70000 THEN 6
        WHEN HOUSEHOLD.INCOME >=70000 AND HOUSEHOLD.INCOME <80000 THEN 7
        WHEN HOUSEHOLD.INCOME >=80000 AND HOUSEHOLD.INCOME <90000 THEN 8
        WHEN HOUSEHOLD.INCOME >=90000 AND HOUSEHOLD.INCOME <100000 THEN 9
        WHEN HOUSEHOLD.INCOME >=100000 AND HOUSEHOLD.INCOME <110000 THEN 10
        WHEN HOUSEHOLD.INCOME >=110000 AND HOUSEHOLD.INCOME <120000 THEN 11
        WHEN HOUSEHOLD.INCOME >=120000 AND HOUSEHOLD.INCOME <130000 THEN 12
        WHEN HOUSEHOLD.INCOME >=130000 AND HOUSEHOLD.INCOME <140000 THEN 13
        WHEN HOUSEHOLD.INCOME >=140000 AND HOUSEHOLD.INCOME <150000 THEN 14
        WHEN HOUSEHOLD.INCOME >=150000 AND HOUSEHOLD.INCOME <175000 THEN 15
        WHEN HOUSEHOLD.INCOME >=175000 AND HOUSEHOLD.INCOME <200000 THEN 16
        WHEN HOUSEHOLD.INCOME >=200000 AND HOUSEHOLD.INCOME <225000 THEN 17
        WHEN HOUSEHOLD.INCOME >=225000 AND HOUSEHOLD.INCOME <250000 THEN 18
        WHEN HOUSEHOLD.INCOME >=250000 AND HOUSEHOLD.INCOME <300000 THEN 19
        WHEN HOUSEHOLD.INCOME >=300000 AND HOUSEHOLD.INCOME <350000 THEN 20
        WHEN HOUSEHOLD.INCOME >=350000 AND HOUSEHOLD.INCOME <400000 THEN 21
        WHEN HOUSEHOLD.INCOME >=400000 AND HOUSEHOLD.INCOME <450000 THEN 22
        WHEN HOUSEHOLD.INCOME >=450000 THEN 23
        ELSE 999 END INC_LVL,

        CASE
        WHEN AGE >=0 AND AGE <12 THEN 0
        WHEN AGE >=12 AND AGE <16 THEN 1
        WHEN AGE >=16 AND AGE <18 THEN 2
        WHEN AGE >=18 AND AGE <22 THEN 3
        WHEN AGE >=22 AND AGE <25 THEN 4
        WHEN AGE >=25 AND AGE <30 THEN 5
        WHEN AGE >=30 AND AGE <35 THEN 6
        WHEN AGE >=35 AND AGE <40 THEN 7
        WHEN AGE >=40 AND AGE <45 THEN 8
        WHEN AGE >=45 AND AGE <50 THEN 9
        WHEN AGE >=50 AND AGE <60 THEN 10
        WHEN AGE >=60 AND AGE <70 THEN 11
        WHEN AGE >=70  THEN 12
        ELSE 999 END AGE_LVL
        FROM TNC_REQUEST LEFT JOIN PERSON ON TNC_REQUEST.PERSON=PERSON.PERSON LEFT JOIN HOUSEHOLD ON PERSON.HOUSEHOLD = HOUSEHOLD.HOUSEHOLD LEFT JOIN LOCATION ON HOUSEHOLD.LOCATION = LOCATION.LOCATION 
        WHERE TNC_REQUEST.ASSIGNED_VEHICLE IS NOT NULL AND TNC_REQUEST.SERVICE_TYPE <> 99
        GROUP BY INC_LVL, PERSONS, WORKERS, AGE_LVL, EDUCATION, GENDER, RACE, ZONE;""",
        "fare_sensitivity_results_vo":f"""
        CREATE TABLE if not exists fare_sensitivity_results_vo AS
        select avg(household.vehicles) as vo, count(household.household) as households, PERSONS, WORKERS, EDUCATION, GENDER, RACE, ZONE.ZONE,
        CASE
        WHEN HOUSEHOLD.INCOME >=0 AND HOUSEHOLD.INCOME <10000 THEN 0
        WHEN HOUSEHOLD.INCOME >=10000 AND HOUSEHOLD.INCOME <20000 THEN 1
        WHEN HOUSEHOLD.INCOME >=20000 AND HOUSEHOLD.INCOME <30000 THEN 2
        WHEN HOUSEHOLD.INCOME >=30000 AND HOUSEHOLD.INCOME <40000 THEN 3
        WHEN HOUSEHOLD.INCOME >=40000 AND HOUSEHOLD.INCOME <50000 THEN 4
        WHEN HOUSEHOLD.INCOME >=50000 AND HOUSEHOLD.INCOME <60000 THEN 5
        WHEN HOUSEHOLD.INCOME >=60000 AND HOUSEHOLD.INCOME <70000 THEN 6
        WHEN HOUSEHOLD.INCOME >=70000 AND HOUSEHOLD.INCOME <80000 THEN 7
        WHEN HOUSEHOLD.INCOME >=80000 AND HOUSEHOLD.INCOME <90000 THEN 8
        WHEN HOUSEHOLD.INCOME >=90000 AND HOUSEHOLD.INCOME <100000 THEN 9
        WHEN HOUSEHOLD.INCOME >=100000 AND HOUSEHOLD.INCOME <110000 THEN 10
        WHEN HOUSEHOLD.INCOME >=110000 AND HOUSEHOLD.INCOME <120000 THEN 11
        WHEN HOUSEHOLD.INCOME >=120000 AND HOUSEHOLD.INCOME <130000 THEN 12
        WHEN HOUSEHOLD.INCOME >=130000 AND HOUSEHOLD.INCOME <140000 THEN 13
        WHEN HOUSEHOLD.INCOME >=140000 AND HOUSEHOLD.INCOME <150000 THEN 14
        WHEN HOUSEHOLD.INCOME >=150000 AND HOUSEHOLD.INCOME <175000 THEN 15
        WHEN HOUSEHOLD.INCOME >=175000 AND HOUSEHOLD.INCOME <200000 THEN 16
        WHEN HOUSEHOLD.INCOME >=200000 AND HOUSEHOLD.INCOME <225000 THEN 17
        WHEN HOUSEHOLD.INCOME >=225000 AND HOUSEHOLD.INCOME <250000 THEN 18
        WHEN HOUSEHOLD.INCOME >=250000 AND HOUSEHOLD.INCOME <300000 THEN 19
        WHEN HOUSEHOLD.INCOME >=300000 AND HOUSEHOLD.INCOME <350000 THEN 20
        WHEN HOUSEHOLD.INCOME >=350000 AND HOUSEHOLD.INCOME <400000 THEN 21
        WHEN HOUSEHOLD.INCOME >=400000 AND HOUSEHOLD.INCOME <450000 THEN 22
        WHEN HOUSEHOLD.INCOME >=450000 THEN 23
        ELSE 999 END INC_LVL,
        CASE
        WHEN AGE >=0 AND AGE <12 THEN 0
        WHEN AGE >=12 AND AGE <16 THEN 1
        WHEN AGE >=16 AND AGE <18 THEN 2
        WHEN AGE >=18 AND AGE <22 THEN 3
        WHEN AGE >=22 AND AGE <25 THEN 4
        WHEN AGE >=25 AND AGE <30 THEN 5
        WHEN AGE >=30 AND AGE <35 THEN 6
        WHEN AGE >=35 AND AGE <40 THEN 7
        WHEN AGE >=40 AND AGE <45 THEN 8
        WHEN AGE >=45 AND AGE <50 THEN 9
        WHEN AGE >=50 AND AGE <60 THEN 10
        WHEN AGE >=60 AND AGE <70 THEN 11
        WHEN AGE >=70  THEN 12
        ELSE 999 END AGE_LVL,
        zone.area_type
        from person left join household on person.household = household.household left join location  on household.location = location.location left join zone on location.zone = zone.zone
        group BY INC_LVL, PERSONS, WORKERS, AGE_LVL, EDUCATION, GENDER, RACE, zone.ZONE, zone.area_type;""",
        "tnc_results_discount":f"""
        CREATE TABLE if not exists tnc_results_discount AS
        SELECT 
        (select sum(1-pooled_service)*1.0/(count(*)*1.0) from tnc_request where service_type <> 99)  as solo_perc,
        (select sum(pooled_service)*1.0/(count(*)*1.0) from tnc_request where service_type <> 99) as pooled_perc,
        (SELECT SUM(fare) *{trip_multiplier} FROM tnc_request) as fare,
        (SELECT AVG(case when b.service_type = 99 then 0 else a.passengers end) FROM tnc_trip a left join tnc_request b  on a.request = b.TNC_request_id) as AVO,
        (SELECT AVG(CASE WHEN a.passengers > 0 THEN a.passengers END) FROM tnc_trip a left join tnc_request b  on a.request = b.TNC_request_id where b.service_type <> 99) as rAVO,
        (SELECT SUM(travel_distance)/1000*0.621371 FROM tnc_trip) as vmt,
        (SELECT SUM(CASE WHEN a.passengers = 0 or service_type = 99 THEN travel_distance END) / SUM(travel_distance) FROM tnc_trip a left join tnc_request b  on a.request = b.TNC_request_id) as evmt_perc,
        (SELECT cast(count(case when a.passengers > 1 then 1 end) as float) / cast(count(*) as float) FROM tnc_trip a left join tnc_request b  on a.request = b.TNC_request_id where b.service_type<>99) as pool_per,
        (SELECT SUM(discount)* {trip_multiplier} FROM tnc_request WHERE pooled_service = 1) as pooled_discount,
        (SELECT count(*) * {trip_multiplier} from tnc_request where service_type <> 99) as requests,
        (SELECT count(*) * {trip_multiplier} from tnc_request where service_type = 99 and assigned_vehicle is not null) as repo_requests,
        (SELECT CAST(count(case when assigned_vehicle is null then 1 end) AS FLOAT) / CAST(count(*) AS FLOAT) FROM tnc_request where service_type <> 99) AS rejection_rate;""",
        "elder_demo":f"""CREATE TABLE if not exists elder_demo AS
        SELECT sum(vehicles) as vehicles, e.zone, b.mode, b.type, CASE WHEN c.age > 60 THEN 2 WHEN c.age < 18 THEN 0 ELSE 1 END AS age_class, 
        COUNT(a.trip_id)  AS trip_count, count(d.household) as household_count,
        sum(a.travel_distance)/1609.34 AS total_travel_distance_miles, sum(a.end-a.start)/60 AS total_ttime_mins, sum(d.income) AS hh_inc_total
        FROM trip a
        LEFT JOIN activity b ON a.trip_id = b.trip
        LEFT JOIN person c ON a.person = c.person
        LEFT JOIN household d ON c.household = d.household
        LEFT JOIN location e ON d.location = e.location
        where a.has_artificial_trip = 0
        GROUP BY e.zone, b.mode, b.type, age_class;""",
        "tnc_stat_summary_helper":f"""create table if not exists tnc_stat_summary_helper as 
        select 
        assigned_vehicle,
        tnc_request_id,
        (pickup_time-request_time)/60 as wait_min, 
        (dropoff_time-pickup_time)/60 as ttime, 
        discount as discount_perc, 
        (discount*fare)*{trip_multiplier} as discount,
        fare*{trip_multiplier} as fare,
        case when (max_pass - party_size) > 0 then 1 else 0 end as pooled,
        eVMT_perc,
        occupied_VMT*{trip_multiplier} as occupied_VMT,
        VMT*{trip_multiplier} as VMT,
        case when service_type =99 then 0 else passengers end as passengers,
        trips*{trip_multiplier} as trips,
        mileage_AVO,
        mileage_rAVO,
        trip_AVO,
        trip_rAVO,
        pooled_service,
        operating_cost*{trip_multiplier} as operating_cost,
        (fare-discount-operating_cost)*{trip_multiplier} as revenue,
        service_type

        from tnc_request a left join 
        (select request,
        eVMT*1.0/VMT*1.0 as eVMT_perc,
        occupied_VMT,
        VMT,
        VMT*0.50 as operating_cost,
        passengers,
        trips,
        passengers*1.0/VMT as mileage_AVO,
        passengers*1.0/occupied_VMT as mileage_rAVO,
        passengers*1.0/trips as trip_AVO,
        passengers*1.0 as trip_rAVO,
        max_pass
        from
        (SELECT
            request,
            SUM(CASE WHEN trip_avo = 0 THEN travel_distance ELSE 0 END) AS eVMT,
            SUM(CASE WHEN trip_avo > 0 THEN travel_distance ELSE 0 END) AS occupied_VMT,
            SUM(travel_distance) as VMT,
            SUM(trip_avo) as passengers,
            COUNT(TNC_Trip_id_int) as trips,
            MAX(trip_avo) as max_pass
        FROM (
            SELECT 
                tnc_trip_id_int, 
                request, 
                passengers*1.0/travel_distance AS mileage_avo, 
                passengers AS trip_avo, 
                travel_distance/1609.34 as travel_distance
            FROM (select 
                a.tnc_trip_id_int, 
                a.request, 
                case when b.service_type = 99 then 0 else a.passengers end as passengers, 
                a.travel_distance, 
                b.service_type 
                from tnc_trip a left join tnc_request b on a.request = b.tnc_request_id)
        ) AS subquery
        GROUP BY request
        ORDER BY request)) b

        on a.tnc_request_id= b.request;""",

    "activity_times": f"""create table if not exists activity_times as
    select sum(a.duration) as activity_duration, a.type as activity_type, case when b.age > 60 then 2 when b.age>18 then 1 else 0 end as age_class, c.zone 
        from activity a 
        left join person b on a.person = b.person 
        left join household d on b.household = d.household
        left join location c on d.location = c.location
        where mode = 'NO_MOVE' group by a.type, age_class, zone;"""
    }
    return queries
