import CU_POLARIS_Postprocessor
from CU_POLARIS_Postprocessor.config import PostProcessingConfig
from CU_POLARIS_Postprocessor import parallel, prerun
from pathlib import Path
from scipy.stats import ttest_ind_from_stats
import pandas as pd

config_wtp = PostProcessingConfig(
    base_dir=Path("C:/Users/jpaul4/Box/Research/Papers/4_WTP Factors Paper/cases_only_critical"),
    fresh_start=False,
    reset_sql=False,
    reset_csvs=False,reset_stops=False)

prerun.pre_run_checks(config_wtp)
parallel.parallel_process_folders(config_wtp)

base_cases = ['atx_du_7','gvl_du_7']
df = config_wtp.results['tnc_stat_summary']
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
                    'comparison_case': folder,
                    'metric': metric,
                    't-statistic': t_stat,
                    'p-value': p_val
                },index=[i])
                print(new_row)
                # Store results
                i=i+1
                results = pd.concat([results,new_row],ignore_index=True)
results.to_csv(config_wtp.base_dir.as_posix() + '/tnc_ttests.csv')
result_hold = config_wtp.results
result_hold['tnc_ttests']=results
config_wtp.update_config(results=result_hold)
