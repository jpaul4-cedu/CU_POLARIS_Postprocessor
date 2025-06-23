# my_package/config.py

from pathlib import Path

class PostProcessingConfig:
    def __init__(self, 
                 fresh_start=False,
                 do_closest_stops=True,
                 reset_sql=False,
                 reset_csvs=False,
                 reset_stops=False,
                 force_skims=False,
                 base_dir=Path('C:\\Users\\jpaul4\\Box\\Research\\Papers\\5_Mode Choice Paper\\cases_only_critical'),
                 scenario_file_names=None,
                 fleet_model_file_names=None,
                 db_names=None,
                 pooling_model_file=None,
                 postprocessing_definitions=None,
                 desired_outputs=None,
                 sql_tables =None,
                 csvs=None,
                 results=None,
                 unique_folders=None,
                 output_h5=False, parallel=True):
        
        self.fresh_start = fresh_start
        self.do_closest_stops = do_closest_stops
        self.reset_sql = reset_sql
        self.reset_csvs = reset_csvs
        self.reset_stops = reset_stops
        self.force_skims = force_skims
        self.parallel = parallel
        
        self.base_dir = base_dir
        self.sql_tables = sql_tables if sql_tables is not None else []
        self.csvs = csvs if csvs is not None else {}
        self.unique_folders = unique_folders if unique_folders is not None else []
        self.results = results if results is not None else {}
        
        self.scenario_file_names = scenario_file_names if scenario_file_names is not None else ['scenario_abm.modified.json','scenario_abm_trajectories.modified.json']
        self.fleet_model_file_names = fleet_model_file_names if fleet_model_file_names is not None else ['SAEVFleetModel_optimization.json']
        self.db_names = db_names if db_names is not None else ['campo', 'greenville']
        self.pooling_model_file = pooling_model_file if pooling_model_file is not None else ['PoolingModel.json']
        
        self.postprocessing_definitions = postprocessing_definitions if postprocessing_definitions is not None else {
            'requests_sum':("process_solo_equiv_fare", {'force_skims': self.force_skims}),
            "closest_stops":("process_nearest_stops", {}),
            "tnc_skim_demo":("process_elder_request_agg", {}),
            "tnc_stat_summary":("process_tnc_stat_summary",{}),
            "demo_financial_case_data":("process_demo_financial_case_data",{}),
            "repositioning_success_rate":("process_tnc_repositioning_success_rate", {})
        }
        
        self.desired_outputs = desired_outputs if desired_outputs is not None else {
            'transit_trip_max_load_helper':'sql',
            'attach':'sql_helper',         
            'transit_trip_max_load':'sql',
            'mode_Distribution_ADULT_Counts':'sql',
            'mode_Distribution_ADULT_Distance':'sql',
            'bus_avo':'sql',
            'pr_avo':'sql',
            'fare_sensitivity_results':'sql',
            'mode_Distribution_ADULT':'sql',
            'distance_tnc_dist':'sql',
            'fare_sensitivity_results_zonal':'sql',
            'fare_sensitivity_demograpic_tnc_stats':'sql',
            'fare_sensitivity_results_vo':'sql',
            'tnc_results_discount':'sql',
            'elder_demo':'sql',
            'requests': 'postprocessing_helper',
            'requests_sum_helper':'postprocessing_helper',
            'requests_sum': 'postprocessing',
            'closest_stops_helper':'postprocessing_helper',
            'closest_stops':'postprocessing',
            'tnc_stat_summary_helper':'postprocessing_helper',
            'tnc_stat_summary':'postprocessing',
            'repositioning_success_rate':'postprocessing',
            'tnc_skim_demo':'postprocessing',
            'tnc_stat_summary_helper':'sql_helper',
            'demo_financial_case_data':'postprocessing',
            'activity_times': 'sql'
        }
        self.output_h5 = output_h5

    def update_config(self, **kwargs):
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)

    
    

    def __repr__(self):
        return (f"PostProcessingConfig(fresh_start={self.fresh_start}, do_closest_stops={self.do_closest_stops}, "
                f"reset_sql={self.reset_sql}, reset_csvs={self.reset_csvs}, reset_stops={self.reset_stops}, "
                f"force_skims={self.force_skims}, base_dir={self.base_dir}, scenario_file_names={self.scenario_file_names}, "
                f"fleet_model_file_names={self.fleet_model_file_names}, db_names={self.db_names}, pooling_model_file={self.pooling_model_file}, "
                f"postprocessing_definitions={self.postprocessing_definitions}, desired_outputs={self.desired_outputs}, sql_tables ={self.sql_tables}, csvs={self.csvs},unique_folders={self.unique_folders},results={self.results})")
