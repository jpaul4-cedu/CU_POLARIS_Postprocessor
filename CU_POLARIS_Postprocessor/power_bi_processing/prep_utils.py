from CU_POLARIS_Postprocessor.config import PostProcessingConfig
from scipy.stats import ttest_ind_from_stats
import pandas as pd
import warnings
import numpy as np
from itertools import zip_longest

def process_folder_names_wtp(config:PostProcessingConfig):
    results={}
    for key, value in config.results.items():
        print(f"Creating case columns for {key}.")
        df = value
            # Split the 'folder' column by '_'
        df[['folder_1', 'folder_2']] = df['folder'].str.split('_', n=1, expand=True)
        df[['folder_2_1', 'folder_2_2']] = df['folder_2'].str.split('_', n=1, expand=True)
        
        # Create 'Iteration' column
        
        # Create 'Strategy' column
        df['Strategy'] = df['folder_2_1'].apply(lambda x: 'Heuristic' if x == 'heur' else 'Proactive')
        
        # Create 'LP Type' column
        df['WTP_Type'] = df['folder_2_1'].apply(lambda x: x if x != 'heur' else 'N/A')
        
        
        # Drop unnecessary columns
        df.drop(columns=['folder_2', 'folder_2_1'], inplace=True)
        
        # Rename columns
        df.rename(columns={'folder_1': 'City', 'folder_2_2': 'Iteration'}, inplace=True)
        results[key]=df
    config.update_config(results=results)

def process_folder_names_rl_repo(config:PostProcessingConfig):
    results={}
    for key, value in config.results.items():
        df = value
        #skip if no rows in the dataframe
        if df.empty:
            warnings.warn(f"No data found for {key}. Skipping folder name processing.")
            #add a city and fleet size column to the dataframe
            df['City'] = 'N/A'
            df['Fleet Size'] = 'N/A'
            df['Repo_Weight'] = 'N/A'
            df['Strategy'] = 'N/A'
            df['Iteration'] = 'N/A'
            results[key]=df
            continue
        print(f"Creating case columns for {key}.")
            # Split the 'folder' column by '_'
        df[['folder_1', 'folder_2']] = df['folder'].str.split('_', n=1, expand=True)
        df[['folder_2_1', 'folder_2_2']] = df['folder_2'].str.split('_', n=1, expand=True)
        df[['folder_2_3', 'folder_2_4']] = df['folder_2_2'].str.split('_', n=1, expand=True)

        #Check if folder_2_2 contains letters or a number by row
        #for rows where folder_2_2 contains letters, set the strategy to those letter codes
        #for rows where folder_2_2 contains a number, set the strategy to 'jar'
        df['Strategy'] = df['folder_2_3'].apply(lambda x: x if x.isalpha() else 'jar')
        df['Repo_Weight'] = df['folder_2_3'].apply(lambda x: x if x.isnumeric() else 'N/A')
        df = df.rename(columns={'folder_1': 'City', 'folder_2_1': 'Fleet Size', 'folder_2_4': 'Iteration'})
        # Drop unnecessary columns
        df.drop(columns=['folder_2_3', 'folder_2_4','folder_2','folder_2_2'], inplace=True)

        results[key]=df
    config.update_config(results=results)

def process_folder_names_cit_fleet_strat(config:PostProcessingConfig):
    results={}
    for key, value in config.results.items():
        df = value
        #skip if no rows in the dataframe
        if df.empty:
            warnings.warn(f"No data found for {key}. Skipping folder name processing.")
            #add a city and fleet size column to the dataframe
            df['City'] = 'N/A'
            df['Fleet Size'] = 'N/A'
            df['Strategy'] = 'N/A'
            df['Iteration'] = 'N/A'
            results[key]=df
            continue
        print(f"Creating case columns for {key}.")
            # Split the 'folder' column by '_'
        df[['folder_1', 'folder_2']] = df['folder'].str.split('_', n=1, expand=True)
        df[['folder_2_1', 'folder_2_2']] = df['folder_2'].str.split('_', n=1, expand=True)
        try:
            df[['folder_2_3', 'folder_2_4']] = df['folder_2_2'].str.split('_', n=1, expand=True)
        except:
            df['folder_2_3']=df['folder_2_2']
            df['folder_2_4']=df['folder_2_2']
        # Create 'Iteration' column
        
        # Create 'Strategy' column
        df['Strategy'] = df['folder_2_3'].apply(lambda x: 'Heuristic' if x == 'heur' else 'Proactive')
        
        # Create 'LP Type' column
        df['Iteration'] = np.where(df['folder_2_4'].notna(), df['folder_2_4'], df['folder_2_3'])

        # Drop unnecessary columns
        df.drop(columns=['folder_2_3', 'folder_2_4','folder_2','folder_2_2','folder_2_3','folder_2_4'], inplace=True)
        
        # Rename columns
        df.rename(columns={'folder_1': 'City', 'folder_2_1': 'Fleet Size'}, inplace=True)
        results[key]=df
    config.update_config(results=results)

def process_tnc_ttests(config:PostProcessingConfig, base_cases=None, base_suffix=None):
    df = config.results['tnc_stat_summary']
    df['city']=df['folder'].apply(lambda x: x[:3])
    df['fleet_size'] = df['folder'].apply(lambda x: x.split('_')[1])

    avg_metrics_t= ['eVMT_perc','wait_min', 'ttime',  'pooled', 'mileage_AVO', 'mileage_rAVO',
                    'trip_AVO', 'trip_rAVO']
        
    sum_metrics_t = ['occupied_VMT', 'VMT', 'discount', 'fare', 'operating_cost', 'revenue']
    sum_metrics = ['assigned_requests','requests']
    i=0
    results =pd.DataFrame()

    if base_cases is not None and base_suffix is not None:
        raise ValueError("Both base case folder and base folder suffix cannot be defined at the same time. Please either use the folder or the suffix as the base case.")

    if base_cases is not None:
        base_df = df[df['folder'].isin(base_cases)]
        for city in df['city'].unique():
            city_df = df[df['city'] == city]
            base_city_df = base_df[base_df['city'] == city]
            for folder in city_df['folder'].unique():
                if folder not in base_city_df['folder']:
                    results, i = run_ttests(avg_metrics_t,sum_metrics_t,sum_metrics,base_city_df,city_df,folder,city,results,i)


    if base_suffix is not None:
        for city in df['city'].unique():
            for fleet_size in df['fleet_size'].unique():
                base_df_city_fleet_size = df[(df['city'] == city) & (df['fleet_size'] == fleet_size) & (df['folder'].str.contains(base_suffix))]
                city_fleet_size_df = df[(df['city'] == city) & (df['fleet_size'] == fleet_size)  & ~(df['folder'].str.contains(base_suffix))]
                folders = city_fleet_size_df['folder'].unique()
                for folder in folders:
                    results, i = run_ttests(avg_metrics_t,sum_metrics_t,sum_metrics,base_df_city_fleet_size,city_fleet_size_df,folder,city,results,i)

    results.to_csv(config.base_dir.as_posix() + '/tnc_ttests.csv')
    result_hold = config.results
    result_hold['tnc_ttests']=results
    config.update_config(results=result_hold)

def run_ttests(avg_metrics_t, sum_metrics_t, sum_metrics, base_df, city_df, folder, city, results,i):
    if base_df.empty:
        Warning(f"Folder {folder} has no base case. Skipping.")
        return results, i
    for metric in avg_metrics_t:
        # Extract stats for both cases
        mean1 = base_df.loc[base_df['Metric']==metric,'Mean'].values[0]
        std1 = base_df.loc[base_df['Metric']==metric,'StdDev'].values[0]
        n1 = base_df.loc[base_df['Metric']==metric,'SampleSize'].values[0]
        #count = base_df.loc[base_df['Metric']==metric,'Sum'].values[0]

        mean2 = city_df.loc[(city_df['Metric'] == metric) & (city_df['folder'] == folder), 'Mean'].values[0]
        std2 = city_df.loc[(city_df['Metric'] == metric) & (city_df['folder'] == folder), 'StdDev'].values[0]
        n2 = city_df.loc[(city_df['Metric'] == metric) & (city_df['folder'] == folder), 'SampleSize'].values[0]
        #count2 = city_df.loc[(city_df['Metric'] == metric) & (city_df['folder'] == folder), 'Sum'].values[0]

        # Perform t-test
        t_stat, p_val = ttest_ind_from_stats(mean1, std1, n1, mean2, std2, n2)
        new_row = pd.DataFrame({
        'city': city,
        'base_case': base_df['folder'].unique()[0],
        'folder': folder,
        'metric': metric,
        't-statistic': t_stat,
        'p-value': p_val, 'value': mean2},index=[i])
        i=i+1
        results = pd.concat([results,new_row],ignore_index=True)
    for metric in sum_metrics_t:
        # Extract stats for both cases
        mean1 = base_df.loc[base_df['Metric']==metric,'Mean'].values[0]
        std1 = base_df.loc[base_df['Metric']==metric,'StdDev'].values[0]
        n1 = base_df.loc[base_df['Metric']==metric,'SampleSize'].values[0]
        #count = base_df.loc[base_df['Metric']==metric,'Sum'].values[0]

        mean2 = city_df.loc[(city_df['Metric'] == metric) & (city_df['folder'] == folder), 'Mean'].values[0]
        std2 = city_df.loc[(city_df['Metric'] == metric) & (city_df['folder'] == folder), 'StdDev'].values[0]
        n2 = city_df.loc[(city_df['Metric'] == metric) & (city_df['folder'] == folder), 'SampleSize'].values[0]
        sum = city_df.loc[(city_df['Metric'] == metric) & (city_df['folder'] == folder), 'Sum'].values[0]

        # Perform t-test
        t_stat, p_val = ttest_ind_from_stats(mean1, std1, n1, mean2, std2, n2)
        new_row = pd.DataFrame({
        'city': city,
        'base_case': base_df['folder'].unique()[0],
        'folder': folder,
        'metric': metric,
        't-statistic': t_stat,
        'p-value': p_val, 'value': sum},index=[i])
        i=i+1
        results = pd.concat([results,new_row],ignore_index=True)
    for metric in sum_metrics:
        count = city_df.loc[(city_df['Metric'] == metric) & (city_df['folder'] == folder), 'Sum'].values[0]
        new_row = pd.DataFrame({
        'city': city,
        'base_case': base_df['folder'].unique()[0],
        'folder': folder,
        'metric': metric,
        'value': count},index=[i])
        i=i+1
        results = pd.concat([results,new_row],ignore_index=True)
    return results, i


def update_h5(config:PostProcessingConfig):
    if config.output_h5:
        with pd.HDFStore(config.base_dir.as_posix()+'/results.h5') as store:
            for key, df in config.results.items():
                store[key] = df
    else:
        warnings.warn("Output H5 is turned off, therefore nothing has been exported.")
    return True

def generate_pbix_control_csv(config:PostProcessingConfig,demo_aggregators:list, case_aggregators:list, folder_to_columns:list):
    df = pd.DataFrame(zip_longest(case_aggregators, folder_to_columns, demo_aggregators), columns=["Case Aggregators", "Folder to Columns", "Demographic Aggregators"])
    df.to_csv(config.base_dir.as_posix() + "/pbix_aggregators.csv")
    result_hold = config.results
    result_hold['pbix_aggregators']=df
    config.update_config(results=result_hold)

        