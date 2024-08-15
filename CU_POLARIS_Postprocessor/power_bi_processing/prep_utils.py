from CU_POLARIS_Postprocessor.config import PostProcessingConfig
from scipy.stats import ttest_ind_from_stats
import pandas as pd
import warnings

def process_folder_names(config:PostProcessingConfig):
    results={}
    for key, value in config.results.items():
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

def process_tnc_ttests(config:PostProcessingConfig, base_cases):
    df = config.results['tnc_stat_summary']
    df['city']=df['folder'].apply(lambda x: x[:3])
    base_df = df[df['folder'].isin(base_cases)]
    results =pd.DataFrame()

    metrics = ['wait_min', 'ttime', 'discount', 'fare', 'pooled', 'eVMT',
                'occupied_VMT', 'VMT', 'mileage_AVO', 'mileage_rAVO',
                'trip_AVO', 'trip_rAVO', 'operating_cost', 'revenue']
    i=0
    for city in df['city'].unique():
        city_df = df[df['city'] == city]
        base_city_df = base_df[base_df['city'] == city]
        for folder in city_df['folder'].unique():
            if folder not in base_city_df['folder']:
                for metric in metrics:
                    # Extract stats for both cases
                    mean1 = base_city_df.loc[base_city_df['Metric']==metric,'Mean'].values[0]
                    std1 = base_city_df.loc[base_city_df['Metric']==metric,'StdDev'].values[0]
                    n1 = base_city_df.loc[base_city_df['Metric']==metric,'SampleSize'].values[0]

                    mean2 = city_df.loc[(city_df['Metric'] == metric) & (city_df['folder'] == folder), 'Mean'].values[0]
                    std2 = city_df.loc[(city_df['Metric'] == metric) & (city_df['folder'] == folder), 'StdDev'].values[0]
                    n2 = city_df.loc[(city_df['Metric'] == metric) & (city_df['folder'] == folder), 'SampleSize'].values[0]

                    # Perform t-test
                    t_stat, p_val = ttest_ind_from_stats(mean1, std1, n1, mean2, std2, n2)
                    new_row = pd.DataFrame({
                    'city': city,
                    'base_case': base_city_df['folder'].unique()[0],
                    'folder': folder,
                    'metric': metric,
                    't-statistic': t_stat,
                    'p-value': p_val},index=[i])
                #print(new_row)
                # Store results
                    i=i+1
                    results = pd.concat([results,new_row],ignore_index=True)
    results.to_csv(config.base_dir.as_posix() + '/tnc_ttests.csv')
    result_hold = config.results
    result_hold['tnc_ttests']=results
    config.update_config(results=result_hold)

def update_h5(config:PostProcessingConfig):
    if config.output_h5:
        with pd.HDFStore(config.base_dir.as_posix()+'/results.h5') as store:
            for key, df in config.results.items():
                store[key] = df
    else:
        warnings.warn("Output H5 is turned off, therefore nothing has been exported.")
    return True

    