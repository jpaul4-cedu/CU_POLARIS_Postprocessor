This package adds a parallel processing architecture for processing outputs from POLARIS (https://www.anl.gov/taps/polaris-transportation-system-simulation-tool).

Please see the jupyter notebook for example usage. The config object holds all the information required by the package to process a folder of POLARIS outputs.

Key config variables:
fresh_start <bool>: trigger the reset of all sql tables and generated CSV files.
do_closest_stops <bool>: runs a ball tree based algorithm to identify the closest bus stops to each household.
reset_sql <bool>: reset sql tables (only run if fresh_start is also true)
reset_csvs <bool>: remove summary CSVS from base folder (only run if fresh_start is also true)
force_skims <bool>: force the rerun of the solo-rideshare pricing algorithm that calculates the solo equivalent fare based on skim travel times and distances
base_dir <pathlib Path object>: base directory containing all simulation result folders. Structure of directory should be:
  /base_dir/case_dirs/iteration_dirs
scenario_file_names <list>: list of scenario file names within your cases that the processor should look for. Defaults to ['scenario_abm.modified.json'] to use data from POLARISLib runs.
fleet_model_file_names <list>: list of fleet model file names within your cases that the processor should look for. Defaults to ['SAEVFleetModel_optimization.json'].
DB_names <list>: prefixes for supply, demand, and result databases (city name). Default is ['greenville','campo']
pooling_model_file <list>: list of pooling model file names within outputs. Default is ['PoolingModel.json']

More complicated config variables: 
postprocessing_definitions <dict>: dictionary of CSV file names and dependent postprocessing functions with args. Entries can be removed to prevent postprocessing scripts from running, or, if new postprocessing is desired, functions can be added to postprocessing.py and their corresponding [csv_name:function,{default_arg:default arg value}] added to this list. Some default args are available within parallel.py to be passed to the postprocessing functions. New postprocessing functions should: write the output dataframe CSV to the base directory, write any intermediate supporting csvs to the iteration folders, add the output DF object to the results dict.

Desired outputs <dict>: dict of desired output csv/dataframe names with a type. Types are as follows:
'sql_helper': intermediate SQL table that is created in each demand DB but does not generate a CSV.
'sql':sql query that generates a CSV and output DF
'postprocessing': python based postprocessing that generates a CSV and DF in results.
'postprocessing_helper': intermediate python postprocessing that generates a CSV within each iteration folder and is used by 'postprocess' tables to generate summary CSV's in the base folder.

Outputs can be removed to prevent their processing from being executed, and added to run them. For SQL and SQL helpers: add table/csv name as key. Define type. Add corresponding sql table create statement to queries.py. Order matters here so be sure if queries are dependent on one another they are ordered appropriatly.

sql_tables <list>: definies sql tables executed within each iteration folder. Populated by running pre_run_checks().

csvs <dict>: dict of each output csv requested, its type from desired_outputs, a boolean value for whether it already exists, and its path if it does exist. Populated by running pre_run_checks().

unique_folders <list>: list of case folders contained in results. If new result files are added when summary CSVs already exist, the user will be prompted to remove them so that the processing can be rerun. Any intermediate sql tables and csvs are retained so they need not be rerun.

results <dict>: dict of output name (from desired_outputs) and generated result dataframes. Can be called to obtain results from postprocessing, or the output CSVS can just be used. 


