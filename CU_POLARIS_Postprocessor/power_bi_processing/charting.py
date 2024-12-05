#import h5py
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import h5py
import sqlite3
import os
from pathlib import Path

def load_h5_table(path, key):
        return pd.read_hdf(path,key=key)

class transit_queries():
    def __init__(self,path,aggregators):
        self.run_transit_queries(path,aggregators)
    def load_transit_trip_max_load(self,path):
        table = load_h5_table(path,'transit_trip_max_load_helper')
        table["weighted_avo"] = table["AVO"]*table["trip_length"]
        return table

    def pattern_avg_avo_agg(self,transit_tab,aggregators):
            # Sort the rows by 'pattern_id'
        sorted_tab = transit_tab.sort_values(by='pattern_id')
        
        # Group the rows by 'pattern_id' and the specified aggregators
        group_cols = [col for col in aggregators['Transit Aggregators'] if col is not None] + ['pattern_id']
        grouped_tab = sorted_tab.groupby(group_cols).agg(
            Max_Load=('max_load', 'max'),
            Max_AVO=('AVO', 'max'),
            Average_Max_Load=('max_load', 'mean'),
            Weighted_AVO_Sum=('weighted_avo', 'sum'),
            Average_Trip_Length=('trip_length', 'mean'),
            Total_Trip_Length=('trip_length', 'sum')
        ).reset_index()
        
        # Add 'Empty Pattern' column
        grouped_tab['Empty_Pattern'] = grouped_tab['Max_Load'] == 0
        
        # Add 'TNC Replaceable Pattern' column
        grouped_tab['TNC_Replaceable_Pattern'] = grouped_tab['Max_Load'] <= 4
        
        # Add 'AVO' column
        grouped_tab['AVO'] = grouped_tab['Weighted_AVO_Sum'] / grouped_tab['Total_Trip_Length']
        
        # Remove unnecessary columns
        grouped_tab = grouped_tab.drop(columns=['Weighted_AVO_Sum', 'Total_Trip_Length'])
        
        # Reorder columns
        #grouped_tab = grouped_tab[['pattern_id', 'Max_Load', 'Max_AVO', 'Average_Max_Load', 'Average_Trip_Length', 'AVO', 'Empty_Pattern', 'TNC_Replaceable_Pattern']]
        
        return grouped_tab

    def trip_avg_avo_agg(self,transit_tab, aggregators):
        # Sort the rows by 'trip_id'
        sorted_tab = transit_tab.sort_values(by='trip_id')
        
        # Group the rows by 'trip_id' and the specified aggregators
        group_cols = [col for col in aggregators['Transit Aggregators'] if col is not None] + ['trip_id']
        grouped_tab = sorted_tab.groupby(group_cols).agg(
            Max_Load=('max_load', 'max'),
            Max_AVO=('AVO', 'max'),
            Average_Max_Load=('max_load', 'mean'),
            Weighted_AVO_Sum=('weighted_avo', 'sum'),
            Average_Trip_Length=('trip_length', 'mean'),
            Total_Trip_Length=('trip_length', 'sum')
        ).reset_index()
        
        # Add 'Empty Trip' column
        grouped_tab['Empty_Trip'] = grouped_tab['Max_Load'] == 0
        
        # Add 'TNC Replaceable Trip' column
        grouped_tab['TNC_Replaceable_Trip'] = grouped_tab['Max_Load'] <= 4
        
        # Add 'AVO' column
        grouped_tab['AVO'] = grouped_tab['Weighted_AVO_Sum'] / grouped_tab['Total_Trip_Length']
        
        # Remove unnecessary columns
        grouped_tab = grouped_tab.drop(columns=['Weighted_AVO_Sum', 'Total_Trip_Length'])
        
        # Reorder columns
        grouped_tab = grouped_tab[['City','Strategy','trip_id', 'Max_Load', 'Max_AVO', 'Average_Max_Load', 'Average_Trip_Length', 'AVO', 'Empty_Trip', 'TNC_Replaceable_Trip']]
        
        return grouped_tab

    def pattern_replaceable_cases(self,transit_tab, aggregators):
        # Sort the rows by 'pattern_id'
        sorted_tab = transit_tab.sort_values(by='pattern_id')
        
        # Group the rows by 'pattern_id', 'folder' and the specified aggregators
        group_cols = [col for col in aggregators['Folder to Columns'] if col is not None] + ['pattern_id', 'folder']
        grouped_tab = sorted_tab.groupby(group_cols).agg(
            Max_Load=('max_load', 'max'),
            Max_AVO=('AVO', 'max'),
            Weighted_AVO_Sum=('weighted_avo', 'sum'),
            Total_Trip_Length=('trip_length', 'sum'),
            Trip_Count=('trip_length', 'count')
        ).reset_index()
        
        # Add 'Empty Pattern' column
        grouped_tab['Empty_Pattern'] = grouped_tab['Max_Load'] == 0
        
        # Add 'TNC Replaceable Pattern' column
        grouped_tab['TNC_Replaceable_Pattern'] = grouped_tab['Max_Load'] <= 4
        
        # Group again by 'TNC Replaceable Pattern', 'Empty Pattern', 'folder' and the specified aggregators
        group_cols = [col for col in aggregators['Folder to Columns'] if col is not None] + ['TNC_Replaceable_Pattern', 'Empty_Pattern', 'folder']
        final_grouped_tab = grouped_tab.groupby(group_cols).agg(
            Count=('Trip_Count', 'sum'),
            Weighted_AVO_Sum=('Weighted_AVO_Sum', 'sum'),
            Distance_Sum=('Total_Trip_Length', 'sum'),
            Max_Load=('Max_Load', 'max'),
            Max_AVO=('Max_AVO', 'max')
        ).reset_index()
        
        return final_grouped_tab

    def pattern_avo_cases(self,transit_tab, aggregators):
        # Group the rows by the specified aggregators
        transit_tab = self.pattern_replaceable_cases(transit_tab, aggregators)
        group_cols = [col for col in aggregators['Folder to Columns'] if col is not None]
        grouped_tab = transit_tab.groupby(group_cols).agg(
            Weighted_AVO_Sum=('Weighted_AVO_Sum', 'sum'),
            Distance_Sum=('Distance_Sum', 'sum'),
            Count=('Count', 'sum'),
            Max_Load=('Max_Load', 'max'),
            Max_AVO=('Max_AVO', 'max')
        ).reset_index()
        
        # Add 'Pattern AVO' column
        grouped_tab['Pattern_AVO'] = grouped_tab['Weighted_AVO_Sum'] / grouped_tab['Distance_Sum']
        
        # Add 'Pattern Distance' column
        grouped_tab['Pattern_Distance'] = grouped_tab['Distance_Sum'] / grouped_tab['Count']
        
        return grouped_tab

    def trip_replaceable_cases(self,transit_tab, aggregators):
        # Sort the rows by 'trip_id'
        sorted_tab = transit_tab.sort_values(by='trip_id')
        
        # Group the rows by 'trip_id', 'folder' and the specified aggregators
        group_cols = [col for col in aggregators['Folder to Columns'] if col is not None] + ['trip_id', 'folder']
        grouped_tab = sorted_tab.groupby(group_cols).agg(
            Max_Load=('max_load', 'max'),
            Max_AVO=('AVO', 'max'),
            Weighted_AVO_Sum=('weighted_avo', 'sum'),
            Total_Trip_Length=('trip_length', 'sum'),
            Trip_Count=('trip_length', 'count')
        ).reset_index()
        
        # Add 'Empty Trip' column
        grouped_tab['Empty_Trip'] = grouped_tab['Max_Load'] == 0
        
        # Add 'TNC Replaceable Trip' column
        grouped_tab['TNC_Replaceable_Trip'] = grouped_tab['Max_Load'] <= 4
        
        # Group again by 'TNC Replaceable Trip', 'Empty Trip', 'folder' and the specified aggregators
        group_cols = [col for col in aggregators['Folder to Columns'] if col is not None] + ['TNC_Replaceable_Trip', 'Empty_Trip', 'folder']
        final_grouped_tab = grouped_tab.groupby(group_cols).agg(
            Count=('Trip_Count', 'sum'),
            Weighted_AVO_Sum=('Weighted_AVO_Sum', 'sum'),
            Distance_Sum=('Total_Trip_Length', 'sum'),
            Max_Load=('Max_Load', 'max'),
            Max_AVO=('Max_AVO', 'max')
        ).reset_index()
        
        return final_grouped_tab

    def trip_avo_cases(self, transit_tab, aggregators):
        # Group the rows by the specified aggregators
        transit_tab = self.trip_replaceable_cases(transit_tab, aggregators)
        group_cols = [col for col in aggregators['Folder to Columns'] if col is not None]
        grouped_tab = transit_tab.groupby(group_cols).agg(
            Weighted_AVO_Sum=('Weighted_AVO_Sum', 'sum'),
            Distance_Sum=('Distance_Sum', 'sum'),
            Count=('Count', 'sum'),
            Max_Load=('Max_Load', 'max'),
            Max_AVO=('Max_AVO', 'max')
        ).reset_index()
        
        # Add 'Trip AVO' column
        grouped_tab['Trip_AVO'] = grouped_tab['Weighted_AVO_Sum'] / grouped_tab['Distance_Sum']
        
        # Add 'Trip Distance' column
        grouped_tab['Trip_Distance'] = grouped_tab['Distance_Sum'] / grouped_tab['Count']
        
        return grouped_tab

    def create_dummy_histogram_values(self):
        #create a dataframe with values of 0 to 5 in increments of 0.1 for two different strategies, "Heuristic" and "Proactive" with values of 0 in a column called "Count"
        dummy_values = pd.DataFrame({'Value Correction': np.arange(0, 5.1, 0.1), 'Count': 0})
        dummy_values['Strategy'] = 'Heuristic'
        dummy_values2 = pd.DataFrame({'Value Correction': np.arange(0, 5.1, 0.1), 'Count': 0})
        dummy_values2['Strategy'] = 'Proactive'
        dummy_values = pd.concat([dummy_values, dummy_values2], ignore_index=True)
        return dummy_values

    def trip_avo_histogram(self,transit_tab, aggregators, dummy_histogram_values):
        # Add 'Greater than 5' column
        transit_tab=self.trip_avg_avo_agg(transit_tab,aggregators)
        transit_tab['Greater_than_5'] = transit_tab['AVO'] > 5
        
        # Round off 'AVO' column
        transit_tab['AVO'] = transit_tab['AVO'].round(1)
        
        # Group the rows by the specified aggregators, 'AVO', and 'Greater than 5'
        group_cols = [col for col in aggregators['Transit Aggregators'] if col is not None] + ['AVO', 'Greater_than_5']
        grouped_tab = transit_tab.groupby(group_cols).size().reset_index(name='Count')
        
        # Add 'Value Correction' column
        grouped_tab['Value_Correction'] = grouped_tab.apply(lambda row: '5.0+' if row['Greater_than_5'] else row['AVO'], axis=1)
        
        # Append dummy histogram values
        appended_tab = pd.concat([grouped_tab, dummy_histogram_values], ignore_index=True)
        
        # Group again by the specified aggregators and 'Value Correction'
        group_cols = [col for col in aggregators['Transit Aggregators'] if col is not None] + ['Value_Correction']
        final_grouped_tab = appended_tab.groupby(group_cols).agg({'Count': 'sum'}).reset_index()
        final_grouped_tab['Value_Correction'] = final_grouped_tab['Value_Correction'].astype(str)
        # Sort the rows by 'Value Correction'
        final_grouped_tab = final_grouped_tab.sort_values(by='Value_Correction')
        
        # Rename 'Value Correction' to 'AVO'
        final_grouped_tab = final_grouped_tab.rename(columns={'Value_Correction': 'AVO'})
        
        return final_grouped_tab

    def pattern_avo_histogram(self,transit_tab, aggregators, dummy_histogram_values):
        # Add 'Greater than 5' column
        transit_tab=self.pattern_avg_avo_agg(transit_tab,aggregators)
        transit_tab['Greater_than_5'] = transit_tab['AVO'] > 5
        
        # Round off 'AVO' column
        transit_tab['AVO'] = transit_tab['AVO'].round(1)
        
        # Group the rows by the specified aggregators, 'AVO', and 'Greater than 5'
        group_cols = [col for col in aggregators['Transit Aggregators'] if col is not None] + ['AVO', 'Greater_than_5']
        grouped_tab = transit_tab.groupby(group_cols).size().reset_index(name='Count')
        
        # Add 'Value Correction' column
        grouped_tab['Value_Correction'] = grouped_tab.apply(lambda row: '5.0+' if row['Greater_than_5'] else row['AVO'], axis=1)
        
        # Append dummy histogram values
        appended_tab = pd.concat([grouped_tab, dummy_histogram_values], ignore_index=True)
        
        # Group again by the specified aggregators and 'Value Correction'
        group_cols = [col for col in aggregators['Transit Aggregators'] if col is not None] + ['Value_Correction']
        final_grouped_tab = appended_tab.groupby(group_cols).agg({'Count': 'sum'}).reset_index()
        final_grouped_tab['Value_Correction'] = final_grouped_tab['Value_Correction'].astype(str)
        
        # Sort the rows by 'Value Correction'
        final_grouped_tab = final_grouped_tab.sort_values(by='Value_Correction')
        
        # Rename 'Value Correction' to 'AVO'
        final_grouped_tab = final_grouped_tab.rename(columns={'Value_Correction': 'AVO'})
        
        return final_grouped_tab

    def transit_pattern_avo_cases(self,transit_tab, aggregators):
        # Group the rows by the specified aggregators
        grouped_tab = self.pattern_avo_cases(transit_tab, aggregators)
        
        # Change the type of 'Pattern AVO' column to numeric
        grouped_tab['Pattern_AVO'] = pd.to_numeric(grouped_tab['Pattern_AVO'])
        
        return grouped_tab

    def transit_trip_avo_cases(self,transit_tab, aggregators):
        # Group the rows by the specified aggregators
        grouped_tab = self.trip_avo_cases(transit_tab, aggregators)
        
        # Change the type of 'Trip AVO' column to numeric
        grouped_tab['Trip_AVO'] = pd.to_numeric(grouped_tab['Trip_AVO'])
        
        return grouped_tab

    def run_transit_queries(self,path, aggregators):
        # Load the tables
        transit_tab = self.load_transit_trip_max_load(path)
        dummy_hist_vals = self.create_dummy_histogram_values()
        self.trip_avo_histogram_vals = self.trip_avo_histogram(transit_tab, aggregators, dummy_hist_vals)
        self.pattern_avo_histogram_vals = self.pattern_avo_histogram(transit_tab, aggregators, dummy_hist_vals)
        self.transit_pattern_avo_cases_vals = self.transit_pattern_avo_cases(transit_tab, aggregators)
        self.transit_trip_avo_cases_vals = self.transit_trip_avo_cases(transit_tab, aggregators)

class mode_shift_queries():
    def __init__(self,path,aggregators):
        self.run_mode_shift_queries(path,aggregators)

    def load_tables(self,path):
        self.bus_avo_df = load_h5_table(path,'bus_avo')
        self.pr_avo_df = load_h5_table(path,'pr_avo')
        self.mda_c_df = load_h5_table(path,'mode_Distribution_ADULT_Counts')
        self.mda_d_df = load_h5_table(path,'mode_Distribution_ADULT_Distance')

    def make_mode_enum(self):
        mode_enum = {"0":"SOV",
                "1":"AUTO_NEST",
                "2":"HOV",
                "3":"TRUCK",
                "4":"BUS",
                "5":"RAIL",
                "6":"NONMOTORIZED_NEST",
                "7":"BICYCLE",
                "8":"WALK",
                "9":"TAXI",
                "10":"SCHOOLBUS",
                "11":"PARK_AND_RIDE",
                "12":"KISS_AND_RIDE",
                "13":"PARK_AND_RAIL",
                "14":"KISS_AND_RAIL",
                "15":"TNC_AND_RIDE",
                "17":"MD_TRUCK",
                "18":"HD_TRUCK",
                "19":"BPLATE",
                "20":"LD_TRUCK",
                "21":"RAIL_NEST",
                "22":"BUS40",
                "23":"BUS60",
                "24":"PNR_BIKE_NEST",
                "25":"RIDE_AND_UNPARK",
                "26":"RIDE_AND_REKISS",
                "27":"RAIL_AND_UNPARK",
                "28":"RAIL_AND_REKISS",
                "29":"MICROM",
                "30":"MICROM_NODOCK",
                "31":"MICROM_AND_TRANSIT",
                "32":"MICROM_NODOCK_AND_TRANSIT",
                "33":"ODDELIVERY",
                "999":"FAIL_MODE",
                "1000":"FAIL_ROUTE",
                "1001":"FAIL_REROUTE",
                "1002":"FAIL_UNPARK",
                "1003":"FAIL_UNPARK2",
                "1004":"FAIL_MODE1",
                "1005":"FAIL_MODE2",
                "1006":"FAIL_MODE3",
                "1007":"FAIL_ROUTE_ACTIVE",
                "1008":"FAIL_ROUTE_WALK_AND_TRANSI",
                "1009":"FAIL_ROUTE_DRIVE_TO_TRANSI",
                "1010":"FAIL_ROUTE_DRIVE_FROM_TRAN",
                "1011":"FAIL_ROUTE_TNC_AND_TRANSIT",
                "1012":"FAIL_ROUTE_TNC",
                "1013":"FAIL_ROUTE_SOV",
                "1014":"FAIL_ROUTE_MICROMOBILITY",
                "1015":"NO_MOVE",
                "9999":"UNSIMULATED"
                }
        return mode_enum
    def pivot_mda(self,mda_df):
        #GET MODE ENUM FROM DICT
        mode_enum = self.make_mode_enum()
        mda_df["mode"] = mda_df["mode"].astype(str).map(mode_enum)

        removed_columns = mda_df.drop(columns=["NHB", "HBW", "HBO"])

      
        # Step 4: Remove another column
        removed_columns1 = removed_columns.drop(columns=["age_class"])

        # Step 5: Pivot the column
        pivoted_column = (
            removed_columns1
            .pivot_table(
                index=removed_columns1.columns.difference(["mode","total"]).tolist(),
                columns="mode",
                values="total",
                aggfunc="sum",
                fill_value=0  # Optional: fill NaN with 0
            )
            .reset_index()  # Optionally reset the index to maintain a DataFrame structure
        )
        return pivoted_column
    
    def mode_combo(self,aggregators):
        source = self.bus_avo_df.copy()
        group_cols = [col for col in aggregators['Folder to Columns'] if col is not None] + ['folder']
        source = source.merge(
            self.pr_avo_df,
            on=group_cols,
            how="left"
        )

        # Expand pr_avo_df columns
        columns_to_expand = ["cnt_pool_y", "dist_pool_y", "cnt_solo_y", "dist_solo_y", "cnt_empt_y", "dist_empt_y"]
        renamed_columns = ["pr." + col.replace("_y","") for col in columns_to_expand]
        source.rename(columns=dict(zip(columns_to_expand, renamed_columns)), inplace=True)
        columns_to_expand = ["cnt_pool_x", "dist_pool_x", "cnt_solo_x", "dist_solo_x", "cnt_empt_x", "dist_empt_x"]
        renamed_columns = [col.replace("_x","") for col in columns_to_expand]
        source.rename(columns=dict(zip(columns_to_expand, renamed_columns)), inplace=True)

        # Replace nulls with 0 in specific columns
        source[["dist_mass", "dist_pool"]] = source[["dist_mass", "dist_pool"]].fillna(0)
        source[["cnt_mass", "cnt_pool"]] = source[["cnt_mass", "cnt_pool"]].fillna(0)

        # Remove unnecessary columns
        dist =source.drop(
            columns=["trip_avo", "cnt_mass", "cnt_pool", "cnt_solo", "cnt_empt"] + renamed_columns
        )
        cnt =source.drop(
            columns=["trip_avo", "dist_mass", "dist_pool", "dist_solo", "dist_empt"] + renamed_columns
        )
        # Merge with Pivot MDA Distances
        
        dist= source.merge(
            self.pivot_mda(self.mda_d_df),
            on=group_cols,
            how="left"
        )
        cnt= source.merge(
            self.pivot_mda(self.mda_c_df),
            on=group_cols,
            how="left"
        )


        # Expand Pivot MDA Distances columns
        mda_columns = ["SOV", "BUS", "RAIL", "BICYCLE", "WALK", "TAXI", "FAIL_REROUTE"]
        dist[mda_columns] = dist[mda_columns]
        cnt[mda_columns] = cnt[mda_columns]

        # Change column types
        dist["RAIL"] = dist["RAIL"].fillna(0).astype(int)
        cnt["RAIL"] = cnt["RAIL"].fillna(0).astype(int)
        dist["BUS"] = dist["BUS"].fillna(0).astype(int)
        cnt["BUS"] = cnt["BUS"].fillna(0).astype(int)

        # Rename columns
        dist.rename(columns={
            "mileage_avo": "Bus AVO",
            "dist_mass": "Bus Mass",
            "dist_pool": "Bus Pool",
            "dist_solo": "Bus Solo",
            "dist_empt": "Bus Empty",
            "pr.dist_pool": "Rideshare Pool",
            "pr.dist_solo": "Rideshare Solo",
            "pr.dist_empt": "Rideshare Empty"
        }, inplace=True)
        cnt.rename(columns={
            "trip_avo": "Bus AVO",
            "cnt_mass": "Bus Mass",
            "cnt_pool": "Bus Pool",
            "cnt_solo": "Bus Solo",
            "cnt_empt": "Bus Empty",
            "pr.cnt_pool": "Rideshare Pool",
            "pr.cnt_solo": "Rideshare Solo",
            "pr.cnt_empt": "Rideshare Empty"
        }, inplace=True)

        # Add calculated columns
        i = 0
        for source in [dist, cnt]:
            source["Bus Total"] = source[["Bus Mass", "Bus Pool", "Bus Solo", "Bus Empty"]].sum(axis=1)
            source["Rideshare Total"] = source[["Rideshare Pool", "Rideshare Solo", "Rideshare Empty"]].sum(axis=1)
            source["Positive"] = source[["Bus Mass", "RAIL", "BICYCLE", "WALK"]].sum(axis=1)
            source["Neutral"] = source[["Bus Pool", "Rideshare Pool"]].sum(axis=1)
            source["Negative"] = source[["Bus Solo", "Bus Empty", "Rideshare Solo", "Rideshare Empty", "SOV"]].sum(axis=1)
            
            if i == 0:
                source["Vehicular Mileage"] = source[[
                "Bus Mass", "Bus Pool", "Bus Solo", "Bus Empty",
                "Rideshare Pool", "Rideshare Solo", "Rideshare Empty", "SOV"
            ]].sum(axis=1)
                source["Rideshare Replaceable Bus Distance"] = source[["Bus Pool", "Bus Solo", "Bus Empty"]].sum(axis=1)
                source["Total Distance"] = source[[
                "Bus Total", "Rideshare Total", "SOV", "RAIL", "BICYCLE", "WALK"
            ]].sum(axis=1)
            else:
                source["Vehicular Trips"] = source[[
                "Bus Mass", "Bus Pool", "Bus Solo", "Bus Empty",
                "Rideshare Pool", "Rideshare Solo", "Rideshare Empty", "SOV"
            ]].sum(axis=1)
                source["Rideshare Replaceable Bus Trips"] = source[["Bus Pool", "Bus Solo", "Bus Empty"]].sum(axis=1)
                source["Total Trips"] = source[[
                "Bus Total", "Rideshare Total", "SOV", "RAIL", "BICYCLE", "WALK"
            ]].sum(axis=1)
            
            # Add share columns
            source["Bus Mass Share"] = source["Bus Mass"] / source["Bus Total"]
            if i == 0:
                source["Rideshare Replaceable Bus Share"] = source["Rideshare Replaceable Bus Distance"] / source["Total Distance"]
                source["SOV Share"] = source["SOV"] / source["Total Distance"]
                source["Rideshare Share"] = source["Rideshare Total"] / source["Total Distance"]
            else:
                source["Rideshare Replaceable Bus Share"] = source["Rideshare Replaceable Bus Trips"] / source["Total Trips"]
                source["SOV Share"] = source["SOV"] / source["Total Trips"]
                source["Rideshare Share"] = source["Rideshare Total"] / source["Total Trips"]

            # Change types for specific columns
            percentage_columns = [
                "SOV Share", "Rideshare Replaceable Bus Share", "Bus Mass Share", "Rideshare Share"
            ]
            source[percentage_columns] = source[percentage_columns].astype(float)
            i += 1

        return dist,cnt

    def mode_comb_proactive(self,mode_dist_combo, mode_dist_combo_heuristic, mode_dist_combo_averages, Aggregators, type):
        # Filter rows where Strategy is "Proactive"
        filtered_rows = mode_dist_combo[mode_dist_combo["Strategy"] == "Proactive"]

        # Merge with mode_dist_combo_heuristic
        merged_queries = pd.merge(
            filtered_rows,
            mode_dist_combo_heuristic,
            how="left",
            on=Aggregators["Case Aggregators"],
            suffixes=("", ".heuristic")
        )
        #update column names from .heuristic suffix to heuristic. prefix
        expanded_heuristic= merged_queries.rename(columns={
            "Positive.heuristic": "heuristic.Positive",
            "Neutral.heuristic": "heuristic.Neutral",
            "Negative.heuristic": "heuristic.Negative",
            "Vehicular Mileage.heuristic": "heuristic.Vehicular Mileage",
            "Vehicular Trips.heuristic": "heuristic.Vehicular Trips",
            "Rideshare Replaceable Bus Distance.heuristic": "heuristic.Rideshare Replaceable Bus Distance",
            "Rideshare Replaceable Bus Trips.heuristic": "heuristic.Rideshare Replaceable Bus Trips",
            "Total Trips.heuristic": "heuristic.Total Trips",
            "Total Distance.heuristic": "heuristic.Total Distance",
            "Bus Mass Share.heuristic": "heuristic.Bus Mass Share",
            "Rideshare Replaceable Bus Share.heuristic": "heuristic.Rideshare Replaceable Bus Share",
            "SOV Share.heuristic": "heuristic.SOV Share",
            "Rideshare Share.heuristic": "heuristic.Rideshare Share"
        })
        # Expand mode_dist_combo_heuristi

        # Add calculated columns
        expanded_heuristic["Positive Shift"] = (expanded_heuristic["Positive"] - expanded_heuristic["heuristic.Positive"]) / expanded_heuristic["heuristic.Positive"]
        expanded_heuristic["Neutral Shift"] = (expanded_heuristic["Neutral"] - expanded_heuristic["heuristic.Neutral"]) / expanded_heuristic["heuristic.Neutral"]
        expanded_heuristic["Negative Shift"] = (expanded_heuristic["Negative"] - expanded_heuristic["heuristic.Negative"]) / expanded_heuristic["heuristic.Negative"]
        expanded_heuristic["Net Overall Shift"] = (
            (expanded_heuristic["Positive"] + expanded_heuristic["Neutral"] + expanded_heuristic["Negative"]) - 
            (expanded_heuristic["heuristic.Positive"] + expanded_heuristic["heuristic.Neutral"] + expanded_heuristic["heuristic.Negative"])
        ) / (expanded_heuristic["heuristic.Positive"] + expanded_heuristic["heuristic.Neutral"] + expanded_heuristic["heuristic.Negative"])
        expanded_heuristic["Net Positive Shift"] = expanded_heuristic["Positive Shift"] - expanded_heuristic["Negative Shift"]
        if type == "dist":
            expanded_heuristic["Vehicular Mileage Shift"] = (expanded_heuristic["Vehicular Mileage"] - expanded_heuristic["heuristic.Vehicular Mileage"]) / expanded_heuristic["heuristic.Vehicular Mileage"]
            columns_to_drop = [
            "heuristic.Positive", "heuristic.Neutral", "heuristic.Negative", "heuristic.Vehicular Mileage"
        ]
            expanded_heuristic["High Capacity Bus Mileage Share Increase"] = expanded_heuristic["Bus Mass Share"] - expanded_heuristic["heuristic.Bus Mass Share"]
            expanded_heuristic["TNC Replaceable Bus Mileage Share Increase"] = expanded_heuristic["Rideshare Replaceable Bus Share"] - expanded_heuristic["heuristic.Rideshare Replaceable Bus Share"]

        else:
            expanded_heuristic["Vehicular Trip Shift"] = (expanded_heuristic["Vehicular Trips"] - expanded_heuristic["heuristic.Vehicular Trips"]) / expanded_heuristic["heuristic.Vehicular Trips"]
            columns_to_drop = [
            "heuristic.Positive", "heuristic.Neutral", "heuristic.Negative", "heuristic.Vehicular Trips"
        ]
            expanded_heuristic["High Capacity Bus Trip Share Increase"] = expanded_heuristic["Bus Mass Share"] - expanded_heuristic["heuristic.Bus Mass Share"]
            expanded_heuristic["TNC Replaceable Bus Trip Share Increase"] = expanded_heuristic["Rideshare Replaceable Bus Share"] - expanded_heuristic["heuristic.Rideshare Replaceable Bus Share"]
        
        # Drop heuristic columns
      
        expanded_heuristic = expanded_heuristic.drop(columns=columns_to_drop)

        # Add more custom columns
        expanded_heuristic["SOV Share Increase"] = expanded_heuristic["SOV Share"] - expanded_heuristic["heuristic.SOV Share"]
        expanded_heuristic["TNC Share Increase"] = expanded_heuristic["Rideshare Share"] - expanded_heuristic["heuristic.Rideshare Share"]
        expanded_heuristic["Net TNC Increase vs SOV"] = expanded_heuristic["TNC Share Increase"] + expanded_heuristic["SOV Share Increase"]

        # Combine with heuristic and averages data

        appended_query = pd.concat([expanded_heuristic, mode_dist_combo_heuristic], ignore_index=True)
        merged_with_averages = pd.merge(
            appended_query,
            mode_dist_combo_averages,
            how="left",
            on=Aggregators["Strategy Aggregators"],
            suffixes=("", ".averages")
        )
        merged_with_averages.rename(columns={
            "Positive.averages": "averages.Positive",
            "Neutral.averages": "averages.Neutral",
            "Negative.averages": "averages.Negative",
            "Vehicular Mileage.averages": "averages.Vehicular Mileage",
            "Vehicular Trips.averages": "averages.Vehicular Trips"
        },inplace=True)

        # Add average shift columns
        merged_with_averages["Positive Shift Avg"] = (merged_with_averages["Positive"] - merged_with_averages["averages.Positive"]) / merged_with_averages["averages.Positive"]
        merged_with_averages["Neutral Shift Avg"] = (merged_with_averages["Neutral"] - merged_with_averages["averages.Neutral"]) / merged_with_averages["averages.Neutral"]
        merged_with_averages["Negative Shift Avg"] = (merged_with_averages["Negative"] - merged_with_averages["averages.Negative"]) / merged_with_averages["averages.Negative"]
        
        if type == "dist":
            merged_with_averages["Vehicular Mileage Shift Avg"] = (merged_with_averages["Vehicular Mileage"] - merged_with_averages["averages.Vehicular Mileage"]) / merged_with_averages["averages.Vehicular Mileage"]
            columns_to_drop_2 = ["averages.Positive", "averages.Negative", "averages.Neutral", "averages.Vehicular Mileage"]
            merged_with_averages.rename(columns={
            "Negative Shift": "Negative Shift Heur",
            "Net Overall Shift": "Net Overall Shift Heur",
            "Net Positive Shift": "Net Positive Shift Heur",
            "Vehicular Mileage Shift": "Vehicular Mileage Shift Heur",
            "Neutral Shift": "Neutral Shift Heur",
            "Positive Shift": "Positive Shift Heur"
        }, inplace=True)
        else:
            merged_with_averages["Vehicular Trip Shift Avg"] = (merged_with_averages["Vehicular Trips"] - merged_with_averages["averages.Vehicular Trips"]) / merged_with_averages["averages.Vehicular Trips"]
            columns_to_drop_2 = ["averages.Positive", "averages.Negative", "averages.Neutral", "averages.Vehicular Trips"]
            merged_with_averages.rename(columns={
            "Negative Shift": "Negative Shift Heur",
            "Net Overall Shift": "Net Overall Shift Heur",
            "Net Positive Shift": "Net Positive Shift Heur",
            "Vehicular Trip Shift": "Vehicular Trip Shift Heur",
            "Neutral Shift": "Neutral Shift Heur",
            "Positive Shift": "Positive Shift Heur"
        }, inplace=True)
        # Drop averages columns
        
        final_result = merged_with_averages.drop(columns=columns_to_drop_2)

        final_result["Net Positive Shift Avg"] = final_result["Positive Shift Avg"] - final_result["Negative Shift Avg"]
        return final_result
    
    def postitive_shift_regression(self,mode_dist_combo_proactive):
        columns_to_keep = [
            "folder","City","Fleet Size","Strategy","Iteration","Net Positive Shift Avg"
        ]
        filtered_data = mode_dist_combo_proactive[columns_to_keep]
        
# Change the type of "Fleet Size" to numeric
        filtered_data["Fleet Size"] = pd.to_numeric(filtered_data["Fleet Size"], errors='coerce')
        return filtered_data

    def run_mode_shift_queries(self,path,aggregators):
        self.load_tables(path)
        dist_combo,cnt_combo=self.mode_combo(aggregators=aggregators)
        self.mode_cnt_combo = cnt_combo
        self.mode_dist_combo = dist_combo
        cnt_combo_heuristic = cnt_combo[cnt_combo["Strategy"] == "Heuristic"]
        dist_combo_heuristic = dist_combo[dist_combo["Strategy"] == "Heuristic"]
        cnt_combo_averages = cnt_combo.groupby(aggregators['Strategy Aggregators']).agg({
                "Positive": "mean",
                "Negative": "mean",
                "Neutral": "mean",
                "Vehicular Trips": "mean"
            }).reset_index()
        dist_combo_averages = dist_combo.groupby(aggregators['Strategy Aggregators']).agg({
                "Positive": "mean",
                "Negative": "mean",
                "Neutral": "mean",
                "Vehicular Mileage": "mean"
            }).reset_index()
        self.mode_dist_combo_proactive = self.mode_comb_proactive(dist_combo, dist_combo_heuristic, dist_combo_averages, aggregators, "dist")
        self.mode_dist_combo_proactive = self.mode_dist_combo_proactive[[
          #  "folder", "Bus AVO", "Bus Mass", "Bus Pool", "Bus Solo", "Bus Empty", "City", "Fleet Size", "Strategy", "Iteration", "Bus Total", "Rideshare Pool", "Rideshare Solo", "Rideshare Empty", "Rideshare Total", "SOV", "RAIL", "BICYCLE", "WALK", "Positive", "Neutral", "Negative", "Vehicular Mileage", "Rideshare Replaceable Bus Distance", "Total Distance", "Bus Mass Share", "Rideshare Replaceable Bus Share", "SOV Share", "Rideshare Share"
            "folder","Bus AVO","Bus Mass","Bus Pool","Bus Solo","Bus Empty","City","Fleet Size","Strategy","Iteration","Bus Total","Rideshare Pool","Rideshare Solo","Rideshare Empty","Rideshare Total","SOV","RAIL","BICYCLE","WALK","Positive","Neutral","Negative","Vehicular Mileage","Rideshare Replaceable Bus Distance","Total Distance","Bus Mass Share","Rideshare Replaceable Bus Share","SOV Share","Rideshare Share","heuristic.Rideshare Replaceable Bus Distance","heuristic.Total Distance","heuristic.Bus Mass Share","heuristic.Rideshare Replaceable Bus Share","heuristic.SOV Share","heuristic.Rideshare Share","Positive Shift Heur","Neutral Shift Heur","Negative Shift Heur","Net Overall Shift Heur","Net Positive Shift Heur","Vehicular Mileage Shift Heur","High Capacity Bus Mileage Share Increase","TNC Replaceable Bus Mileage Share Increase","SOV Share Increase","TNC Share Increase","Net TNC Increase vs SOV","Positive Shift Avg","Neutral Shift Avg","Negative Shift Avg","Vehicular Mileage Shift Avg","Net Positive Shift Avg"
        ]]
        self.mode_cnt_combo_proactive = self.mode_comb_proactive(cnt_combo, cnt_combo_heuristic, cnt_combo_averages, aggregators, "cnt")
        self.mode_cnt_combo_proactive = self.mode_cnt_combo_proactive[[
            #"folder", "Bus AVO", "Bus Mass", "Bus Pool", "Bus Solo", "Bus Empty", "City", "Fleet Size", "Strategy", "Iteration", "Bus Total", "Rideshare Pool", "Rideshare Solo", "Rideshare Empty", "Rideshare Total", "SOV", "RAIL", "BICYCLE", "WALK", "Positive", "Neutral", "Negative", "Vehicular Trips", "Rideshare Replaceable Bus Trips", "Total Trips", "Bus Mass Share", "Rideshare Replaceable Bus Share", "SOV Share", "Rideshare Share"
            "folder","Bus AVO","Bus Mass","Bus Pool","Bus Solo","Bus Empty","City","Fleet Size","Strategy","Iteration","Bus Total","Rideshare Pool","Rideshare Solo","Rideshare Empty","Rideshare Total","SOV","RAIL","BICYCLE","WALK","Positive","Neutral","Negative","Vehicular Trips","Rideshare Replaceable Bus Trips","Total Trips","Bus Mass Share","Rideshare Replaceable Bus Share","SOV Share","Rideshare Share","heuristic.Rideshare Replaceable Bus Trips","heuristic.Total Trips","heuristic.Bus Mass Share","heuristic.Rideshare Replaceable Bus Share","heuristic.SOV Share","heuristic.Rideshare Share","Positive Shift Heur","Neutral Shift Heur","Negative Shift Heur","Net Overall Shift Heur","Net Positive Shift Heur","Vehicular Trip Shift Heur","High Capacity Bus Trip Share Increase","TNC Replaceable Bus Trip Share Increase","SOV Share Increase","TNC Share Increase","Net TNC Increase vs SOV","Positive Shift Avg","Neutral Shift Avg","Negative Shift Avg","Vehicular Trip Shift Avg","Net Positive Shift Avg"
        ]]
        self.positive_shift_reg = self.postitive_shift_regression(self.mode_dist_combo_proactive)

class demographic_queries():
    def __init__(self,path,aggregators,db_path, city_names):
        self.run_demographic_queries(path,aggregators,db_path,city_names)

    def load_tables(self,path):
        self.demographic_df = load_h5_table(path,'fare_sensitivity_demograpic_tnc_stats')
        self.zone_demographic_df = load_h5_table(path,'elder_demo')
        self.activity_times_df = load_h5_table(path,'activity_times')
        self.tnc_skim_demo_df = load_h5_table(path,'tnc_skim_demo')
        self.closest_stops_by_zone_df = load_h5_table(path,'closest_stops')
        
    
    def zone_data(self,aggregators):
        dataset = self.zone_demographic_df.copy()
        filtered_rows = dataset[dataset['age_class'] == 2]
    
        # Change column types
        filtered_rows = filtered_rows.astype({
            'zone': 'int64',
            'mode': 'str',
            'type': 'str',
            'age_class': 'int64',
            'folder': 'str',
            'vehicles': 'float64',
            'trip_count': 'int64',
            'total_travel_distance_miles': 'float64',
            'total_ttime_mins': 'float64',
            'hh_inc_total': 'int64',
            'household_count': 'int64'
        })
        
        # Filter rows where zone is not null
        filtered_rows = filtered_rows[filtered_rows['zone'].notnull()]
        
        # Add 'Age Group' column
        filtered_rows['Age Group'] = filtered_rows['age_class'].apply(lambda x: 'Senior' if x == 2 else ('Adult' if x == 1 else 'Child'))
        
        # Change column types again
        filtered_rows = filtered_rows.astype({
            'total_travel_distance_miles': 'float64',
            'total_ttime_mins': 'float64',
            'hh_inc_total': 'float64',
            'household_count': 'int64',
            'trip_count': 'int64',
            'vehicles': 'int64',
            'zone': 'str'
        })
        
        return filtered_rows

    def zone_income_check(self, aggregators):
            # Group rows by specified columns and calculate sum of household_count and hh_inc_total
        group_cols = [col for col in aggregators['Folder to Columns'] if col is not None] + ['zone']
        grouped_rows = self.zone_data(aggregators).groupby(group_cols).agg({
            'household_count': 'sum',
            'hh_inc_total': 'sum'
        }).reset_index()
        
        # Add 'avg_hh_inc' column
        grouped_rows['avg_hh_inc'] = grouped_rows['hh_inc_total'] / grouped_rows['household_count']
        
        # Remove 'household_count' and 'hh_inc_total' columns
        grouped_rows = grouped_rows.drop(columns=['household_count', 'hh_inc_total'])
        
        # Add 'HH_Inc_Quartile' column
        grouped_rows['HH_Inc_Quartile'] = grouped_rows['avg_hh_inc'].apply(
            lambda x: '4- >75%' if x > 95340 else ('3- between 50% and 75%' if x > 51906 else ('2- between 25% and 50 %' if x > 26339 else '1- <25%'))
        )
        
        return grouped_rows

    def mode_data(self,aggregators):
        zone_data_df = self.zone_data(aggregators)
        filtered_rows = zone_data_df[(zone_data_df['age_class'] == 2) & (zone_data_df['mode'] != 'FAIL_REROUTE')]
    
        # Add 'average travel distance' column
        filtered_rows['average travel distance'] = filtered_rows['total_travel_distance_miles'] / filtered_rows['trip_count']
        
        # Add 'average travel time' column
        filtered_rows['average travel time'] = filtered_rows['total_ttime_mins'] / filtered_rows['trip_count']
        
        # Change column types
        filtered_rows = filtered_rows.astype({
            'average travel distance': 'float64',
            'average travel time': 'float64'
        })
        
        # Merge with zones_income_check
        merge_cols = [col for col in aggregators['Transit Aggregators'] if col is not None] + ['zone']
        merged_data = filtered_rows.merge(self.zone_income_check(aggregators), on=merge_cols, how='left')
        
        # Expand 'HH_Inc_Quartile' column
        merged_data = merged_data.rename(columns={'HH_Inc_Quartile': 'HH_Inc_Quartile'})
        merged_data.drop(columns=['Fleet Size_y'], inplace=True)
        merged_data.rename(columns={'Fleet Size_x': 'Fleet Size'}, inplace=True)
        return merged_data

    def hh_inc_map(self,aggregators):
        group_cols = [col for col in aggregators['Demographic Aggregators'] if col is not None] + ['zone']
        grouped_rows = self.mode_data(aggregators).groupby(group_cols).agg({
        'hh_inc_total': 'sum',
        'household_count': 'sum'
        }).reset_index()
        
        # Add 'Senior HH Income Average' column
        grouped_rows['Senior HH Income Average'] = grouped_rows['hh_inc_total'] / grouped_rows['household_count']
        
        # Add 'HH Inc Quartile' column
        grouped_rows['HH Inc Quartile'] = grouped_rows['Senior HH Income Average'].apply(
            lambda x: '4- >75%' if x > 95340 else ('3- between 50% and 75%' if x > 51906 else ('2- between 25% and 50%' if x > 26339 else '1- <25%'))
        )

        
        self.hh_inc_map_df = grouped_rows
        self.hh_inc_map_gsc = self.hh_inc_map_df[self.hh_inc_map_df['City'] == 'gsc']
        self.hh_inc_map_atx = self.hh_inc_map_df[self.hh_inc_map_df['City'] == 'atx']

    def agg_mode_data(self,aggregators):
        group_cols = [col for col in aggregators['Folder to Columns'] if col is not None] + ['mode', 'type']
        grouped_rows = self.mode_data(aggregators).groupby(group_cols).agg({
            'trip_count': 'sum',
            'household_count': 'sum',
            'total_travel_distance_miles': 'sum',
            'total_ttime_mins': 'sum',
            'hh_inc_total': 'sum'
        }).reset_index()
        
        # Add 'avg_hh_inc' column
        grouped_rows['avg_hh_inc'] = grouped_rows['hh_inc_total'] / grouped_rows['household_count']
        
        # Change column types
        grouped_rows = grouped_rows.astype({
            'avg_hh_inc': 'float64'
        })
        
        # Add 'HH_Inc_Quartile' column
        grouped_rows['HH_Inc_Quartile'] = grouped_rows['avg_hh_inc'].apply(
            lambda x: '4- >75%' if x > 95340 else ('3- between 50% and 75%' if x > 51906 else ('2- between 25% and 50 %' if x > 26339 else '1- <25%'))
        )
        
        # Add 'average travel time' column
        grouped_rows['average travel time'] = grouped_rows['total_ttime_mins'] / grouped_rows['trip_count']
        
        # Add 'average travel distance' column
        grouped_rows['average travel distance'] = grouped_rows['total_travel_distance_miles'] / grouped_rows['trip_count']
        
        # Change column types again
        grouped_rows = grouped_rows.astype({
            'average travel time': 'float64',
            'average travel distance': 'float64'
        })
        
        # Rename columns
        grouped_rows = grouped_rows.rename(columns={
            'total_ttime_mins': 'Total Travel Time',
            'trip_count': 'Trip Count',
            'total_travel_distance_miles': 'Travel Distance',
            'HH_Inc_Quartile': 'Household Income Quartile'
        })
        
        return grouped_rows

    def tnc_request_demo(self,aggregators):
            # Change column types
        dataset = self.tnc_skim_demo_df.copy()
        dataset = dataset.astype({
            'total discount': 'float64',
            'total fare': 'float64',
            'total skim solo fare': 'float64',
            'income': 'float64',
            'total discount percentage': 'float64',
            'request_count': 'int64'
        })
        
        # Group rows by specified columns and calculate sum of request_count, total skim solo fare, total discount, total fare, and total discount percentage
        group_cols = [col for col in aggregators['Folder to Columns'] if col is not None] + ['zone', 'folder', 'age_class']
        grouped_rows = dataset.groupby(group_cols).agg({
            'request_count': 'sum',
            'total skim solo fare': 'sum',
            'total discount': 'sum',
            'total fare': 'sum',
            'total discount percentage': 'sum'
        }).reset_index()
        
        # Add 'mode' column
        grouped_rows['mode'] = 'TAXI'
        
        # Change column types again
        grouped_rows = grouped_rows.astype({
            'zone': 'int64',
            'City': 'str',
            'Strategy': 'str',
            'age_class': 'int64',
            'request_count': 'int64',
            'total skim solo fare': 'float64',
            'total discount': 'float64',
            'total fare': 'float64',
            'total discount percentage': 'float64',
            'mode': 'str'
        })
        
        return grouped_rows

    def demo_zone_summary_tnc(self,aggregators,db_path,city_names):
        merged_data = self.zone_master(db_path,city_names).merge(self.zone_income_check(aggregators), on=['City', 'zone'], how='left')
    
        # Sort rows by 'zone'
        merged_data = merged_data.sort_values(by='zone')
        
        # Merge with tnc_request_demo_agg
        merge_cols = [col for col in aggregators['Folder to Columns'] if col is not None]
        final_data = merged_data.merge(self.tnc_request_demo_agg(aggregators), on=merge_cols, how='left')
        
        return final_data

    def tnc_request_demo_agg(self,aggregators):
        tnc_request_demo = self.tnc_request_demo(aggregators)
        filtered_rows = tnc_request_demo[tnc_request_demo['age_class'] == 2]

        filtered_rows.rename(columns={
            'request_count': 'Request Count',
            'total skim solo fare': 'Total Solo Skim Fare',
            'total discount': 'Total Discount',
            'total fare': 'Total Fare',
            'total discount percentage': 'Total Discount %'
            }, inplace=True)
        
        # Change column types
        filtered_rows = filtered_rows.astype({
            'zone': 'int64',
            'City': 'str',
            'Fleet Size': 'int64',
            'Strategy': 'str',
            'age_class': 'int64',
            'Request Count': 'int64',
            'Total Solo Skim Fare': 'float64',
            'Total Discount': 'float64',
            'Total Fare': 'float64',
            'Total Discount %': 'float64',
            'mode': 'str'
        })
        
        
        # Add 'Fare per request' column
        filtered_rows['Fare per request'] = filtered_rows['Total Fare'] / filtered_rows['Request Count']
        
        # Add 'discount % per request' column
        filtered_rows['discount % per request'] = filtered_rows['Total Discount %'] / filtered_rows['Request Count']
        
        # Change column types again
        filtered_rows = filtered_rows.astype({
            'discount % per request': 'float64',
            'Fare per request': 'float64',
            'Fleet Size': 'str'
        })
        
        return filtered_rows

    def agg_tnc_case(self,aggregators,db_path,city_names):
        group_cols = [col for col in aggregators['Folder to Columns'] if col is not None] + ['HH_Inc_Quartile']
        grouped_rows = self.demo_zone_summary_tnc(aggregators,db_path,city_names).groupby(group_cols).agg({
            'Total Solo Skim Fare': 'sum',
            'Total Discount': 'sum',
            'Total Fare': 'sum',
            'Total Discount %': 'sum',
            'Request Count': 'sum'
        }).reset_index()
        
        # Add 'Average Fare' column
        grouped_rows['Average Fare'] = grouped_rows['Total Fare'] / grouped_rows['Request Count']
        
        # Add 'Average Discount %' column
        grouped_rows['Average Discount %'] = grouped_rows['Total Discount %'] / grouped_rows['Request Count']
        
        return grouped_rows

    def zone_master(self, db_path, city_names:dict):
        # load the supply sqlite database
        comb_zone_df = None
        for city,prefix in city_names.items():
            path = os.path.join(db_path, f'{city}-Supply.sqlite')
            with sqlite3.connect(path) as conn:
                # load the zone table
                zone_df = pd.read_sql_query("SELECT zone FROM zone order by zone", conn)
                # load the zone demographic table
                zone_df["City"] = prefix
                if comb_zone_df is None:
                    comb_zone_df = zone_df
                else:
                    comb_zone_df = pd.concat([comb_zone_df, zone_df])
        comb_zone_df["zone"] = comb_zone_df["zone"].astype(str)
        return comb_zone_df

    def demo_agg_tnc_case(self,aggregators,db_path,city_names):
        group_cols = [col for col in aggregators['Folder to Columns'] if col is not None] + ['HH_Inc_Quartile']
        grouped_rows = self.demo_zone_summary_tnc(aggregators,db_path,city_names).groupby(group_cols).agg({
            'Total Solo Skim Fare': 'sum',
            'Total Discount': 'sum',
            'Total Fare': 'sum',
            'Total Discount %': 'sum',
            'Request Count': 'sum'
        }).reset_index()
        
        # Add 'Average Fare' column
        grouped_rows['Average Fare'] = grouped_rows['Total Fare'] / grouped_rows['Request Count']
        
        # Add 'Average Discount %' column
        grouped_rows['Average Discount %'] = grouped_rows['Total Discount %'] / grouped_rows['Request Count']
        
        self.demo_agg_tnc_case_df = grouped_rows
        return grouped_rows

    def demo_agg_mode_data(self,aggregators):
        group_cols = [col for col in aggregators['Folder to Columns'] if col is not None] + ['mode', 'type']
        grouped_rows = self.mode_data(aggregators).groupby(group_cols).agg({
            'trip_count': 'sum',
            'household_count': 'sum',
            'total_travel_distance_miles': 'sum',
            'total_ttime_mins': 'sum',
            'hh_inc_total': 'sum'
        }).reset_index()
        
        # Add 'avg_hh_inc' column
        grouped_rows['avg_hh_inc'] = grouped_rows['hh_inc_total'] / grouped_rows['household_count']
        
        # Change column types
        grouped_rows = grouped_rows.astype({
            'avg_hh_inc': 'float64'
        })
        
        # Add 'HH_Inc_Quartile' column
        grouped_rows['HH_Inc_Quartile'] = grouped_rows['avg_hh_inc'].apply(
            lambda x: '4- >75%' if x > 95340 else ('3- between 50% and 75%' if x > 51906 else ('2- between 25% and 50 %' if x > 26339 else '1- <25%'))
        )
        
        # Add 'average travel time' column
        grouped_rows['average travel time'] = grouped_rows['total_ttime_mins'] / grouped_rows['trip_count']
        
        # Add 'average travel distance' column
        grouped_rows['average travel distance'] = grouped_rows['total_travel_distance_miles'] / grouped_rows['trip_count']
        
        # Change column types again
        grouped_rows = grouped_rows.astype({
            'average travel time': 'float64',
            'average travel distance': 'float64'
        })
        
        # Rename columns
        grouped_rows = grouped_rows.rename(columns={
            'total_ttime_mins': 'Total Travel Time',
            'trip_count': 'Trip Count',
            'total_travel_distance_miles': 'Travel Distance',
            'HH_Inc_Quartile': 'Household Income Quartile'
        })
        
        self.demo_agg_mode_data_df = grouped_rows

    def zone_mode_proportions(self,aggregators):
        groups = [col for col in aggregators['Folder to Columns'] if col is not None] + ['zone','type','mode']
        grouped_rows = self.mode_data(aggregators).groupby(groups).agg({
        'total_ttime_mins': 'sum',
        'total_travel_distance_miles': 'sum',
        'trip_count': 'sum'
        }).reset_index()

        grouped_rows.rename(columns={
            'total_ttime_mins': 'Travel Time', 
            'total_travel_distance_miles': 'Travel Distance', 
            'trip_count': 'Trips'
            }, inplace=True)
        
        # Change column types
        grouped_rows['zone'] = grouped_rows['zone'].astype(int)
        
        
        # Add 'fold_zone_type' column
       # grouped_rows['fold_zone_type'] = grouped_rows['folder'] + "_" + grouped_rows['zone'].astype(str) + "^" + grouped_rows['type']
        
        # Remove 'zone', 'folder', and 'type' columns
        #grouped_rows = grouped_rows.drop(columns=['zone', 'folder', 'type'])
        
        # Reorder columns
        #grouped_rows = grouped_rows[['fold_zone_type', 'mode', 'total_ttime_mins', 'total_travel_distance_miles', 'trip_count']]
        
        # Unpivot columns
        unpivoted_rows = grouped_rows.melt(id_vars=groups, var_name='Attribute', value_name='Value')
        
        # Add 'mode_attrib' column
        unpivoted_rows['mode_attrib'] = unpivoted_rows['mode'] + "_" + unpivoted_rows['Attribute']
        
        groups.remove('mode')
        #cols = unpivoted_rows['mode_attrib'].unique().tolist()
        
        # Pivot column
        pivoted_rows = unpivoted_rows.pivot_table(index = groups, columns=['mode_attrib'], values='Value', aggfunc='sum').reset_index()
        
   
        return pivoted_rows

    def closest_stops_by_zone(self,aggregators):
        # Change column types
        dataset = self.closest_stops_by_zone_df.copy()
        dataset = dataset.astype({
            'zone': 'str',
            'under_60': 'int64',
            'over_60': 'int64',
            'vehicles': 'int64',
            'walkable': 'int64'
        })
        
        # Filter rows where 'over_60' is greater than 0
        filtered_rows = dataset[dataset['over_60'] > 0]
        
        # Change column types again
        filtered_rows = filtered_rows.astype({
            'count': 'int64',
            'total_income': 'float64'
        })
        
        # Rename 'walkable' to 'Walking Distance to Bus Stop'
        filtered_rows = filtered_rows.rename(columns={'walkable': 'Walking Distance to Bus Stop'})
        
        return filtered_rows

    def demo_activity_time_usage(self,aggregators): 
        elder_activity_times = self.activity_times_df.copy()
        elder_activity_times.rename(columns={'activity_duration': 'duration'}, inplace=True)
        elder_activity_times['zone'] = elder_activity_times['zone'].astype(int)
    
        # Filter rows where age_class is 2
        filtered_rows = elder_activity_times[elder_activity_times['age_class'] == 2]
        
        # Merge with zone_mode_proportions
        merge_cols = ([col for col in aggregators['Folder to Columns'] if col is not None] + ['zone'])
        merged_data = filtered_rows.merge(self.zone_mode_proportions(aggregators), left_on=merge_cols, right_on=merge_cols, how='left')
        
        # Group rows by specified columns and calculate sum of various columns
        group_cols = [col for col in aggregators['Folder to Columns'] if col is not None] + ['type']
        grouped_rows = merged_data.groupby(group_cols).agg({
            'duration': 'sum',
            'SOV_Travel Time': 'sum',
            'SOV_Trips': 'sum',
            'TAXI_Travel Time': 'sum',
            'TAXI_Trips': 'sum',
            'BUS_Travel Time': 'sum',
            'BUS_Trips': 'sum',
            'RAIL_Travel Time': 'sum',
            'RAIL_Trips': 'sum'
        }).reset_index()
        
        # Replace null values with 0
        grouped_rows.fillna(0, inplace=True)
        
        # Add 'Total Travel Time' column
        grouped_rows['Total Travel Time'] = grouped_rows['SOV_Travel Time'] + grouped_rows['TAXI_Travel Time'] + grouped_rows['BUS_Travel Time'] + grouped_rows['RAIL_Travel Time']
        
        # Add 'Total Trips' column
        grouped_rows['Total Trips'] = grouped_rows['SOV_Trips'] + grouped_rows['TAXI_Trips'] + grouped_rows['BUS_Trips'] + grouped_rows['RAIL_Trips']
        
        # Filter rows where type is not 'HOME' or 'WORK AT HOME'
        filtered_rows = grouped_rows[(grouped_rows['type'] != 'HOME') & (grouped_rows['type'] != 'WORK AT HOME')]
        
        # Add share columns
        filtered_rows['SOV Share'] = filtered_rows['SOV_Travel Time'] / filtered_rows['Total Travel Time']
        filtered_rows['TNC Share'] = filtered_rows['TAXI_Travel Time'] / filtered_rows['Total Travel Time']
        filtered_rows['Bus Share'] = filtered_rows['BUS_Travel Time'] / filtered_rows['Total Travel Time']
        filtered_rows['Rail Share'] = filtered_rows['RAIL_Travel Time'] / filtered_rows['Total Travel Time']
        
        # Add 'avg_duration_per_trip' column
        filtered_rows['avg_duration_per_trip'] = filtered_rows['duration'] / filtered_rows['Total Trips']
        
        # Add time budget columns
        filtered_rows['TNC Time Budget'] = 1 - (filtered_rows['TAXI_Travel Time'] / (filtered_rows['duration'] * (filtered_rows['TAXI_Trips'] / filtered_rows['Total Trips'])))
        filtered_rows['SOV Time Budget'] = 1 - (filtered_rows['SOV_Travel Time'] / (filtered_rows['duration'] * (filtered_rows['SOV_Trips'] / filtered_rows['Total Trips'])))
        filtered_rows['Bus Time Budget'] = 1 - (filtered_rows['BUS_Travel Time'] / (filtered_rows['duration'] * (filtered_rows['BUS_Trips'] / filtered_rows['Total Trips'])))
        filtered_rows['Rail Time Budget'] = 1 - (filtered_rows['RAIL_Travel Time'] / (filtered_rows['duration'] * (filtered_rows['RAIL_Trips'] / filtered_rows['Total Trips'])))
        
        # Add time budget level columns
        filtered_rows['TNC Time Budget Level'] = filtered_rows['TNC Time Budget'].apply(lambda x: 'No Data' if pd.isnull(x) else (1 if x > 0.8 else (2 if x > 0.6 else (3 if x > 0.4 else 4))))
        filtered_rows['SOV Time Budget Level'] = filtered_rows['SOV Time Budget'].apply(lambda x: 'No Data' if pd.isnull(x) else (1 if x > 0.8 else (2 if x > 0.6 else (3 if x > 0.4 else 4))))
        filtered_rows['Bus Time Budget Level'] = filtered_rows['Bus Time Budget'].apply(lambda x: 'No Data' if pd.isnull(x) else (1 if x > 0.8 else (2 if x > 0.6 else (3 if x > 0.4 else 4))))
        filtered_rows['Rail Time Budget Level'] = filtered_rows['Rail Time Budget'].apply(lambda x: 'No Data' if pd.isnull(x) else (1 if x > 0.8 else (2 if x > 0.6 else (3 if x > 0.4 else 4))))
        
        # Change column types again
        filtered_rows = filtered_rows.astype({
            'Rail Time Budget': 'float64',
            'Bus Time Budget': 'float64',
            'SOV Time Budget': 'float64',
            'TNC Time Budget': 'float64',
            'avg_duration_per_trip': 'float64',
            'Rail Share': 'float64',
            'Bus Share': 'float64',
            'TNC Share': 'float64',
            'SOV Share': 'float64',
            'Total Travel Time': 'float64',
            'Total Trips': 'float64'
        })
        
        self.demo_activity_time_usage_df =  filtered_rows

    def closest_stops_helper(self,aggregators):
        grouped_rows = self.closest_stops_by_zone(aggregators).groupby(['Walking Distance to Bus Stop', 'zone', 'City']).agg({
        'count': 'sum',
            'total_income': 'sum'
        }).reset_index()
        
        # Remove 'Total Income' column
        grouped_rows = grouped_rows.drop(columns=['total_income'])
        
        # Change column types
        grouped_rows['zone'] = grouped_rows['zone'].astype(str)
        grouped_rows['Walking Distance to Bus Stop'] = grouped_rows['Walking Distance to Bus Stop'].astype(bool)
        
        return grouped_rows

    def hh_inc_hist(self,aggregators):
        # Remove 'HH Inc Quartile' column
        hh_inc_map = self.hh_inc_map_df.copy()
        hh_inc_map = hh_inc_map.drop(columns=['HH Inc Quartile'])
        
        # Add 'Rounded w/ Upper Bound' column
        hh_inc_map['Rounded w/ Upper Bound'] = hh_inc_map['Senior HH Income Average'].apply(lambda x: '1.5+' if x > 150000 else 0)
        
        # Add 'Rounded Income' column
        hh_inc_map['Rounded Income'] = hh_inc_map['Senior HH Income Average'].apply(lambda x: str(round(x / 5000) * 5 / 100))
        
        # Add 'Rounded Income Final' column
        hh_inc_map['Rounded Income Final'] = hh_inc_map.apply(lambda row: row['Rounded Income'] if row['Rounded w/ Upper Bound'] == 0 else row['Rounded w/ Upper Bound'], axis=1)
        
        # Remove 'Rounded Income' column
        hh_inc_map = hh_inc_map.drop(columns=['Rounded Income'])
        
        # Rename 'Rounded Income Final' to 'Rounded Income'
        hh_inc_map = hh_inc_map.rename(columns={'Rounded Income Final': 'Rounded Income'})
        
        # Merge with closest_stops_by_zone
        merged_data = hh_inc_map.merge(self.closest_stops_helper(aggregators), on=['zone', 'City'], how='left')
        
        # Group rows by 'Rounded Income', 'City', and 'Walking Distance to Bus Stop' and calculate sum of 'HH_Count' and 'dist.Count'
        grouped_rows = merged_data.groupby(['Rounded Income', 'City', 'Walking Distance to Bus Stop']).agg({
            'household_count': 'sum',
            'count': 'sum'
        }).reset_index()
        
        grouped_rows.rename(columns={'household_count': "House Hold Count"},inplace=True)
        # Change column types
        grouped_rows['Walking Distance to Bus Stop'] = grouped_rows['Walking Distance to Bus Stop'].astype(str)
        
        # Replace null values with 'n/a'
        grouped_rows['Walking Distance to Bus Stop'] = grouped_rows['Walking Distance to Bus Stop'].fillna('n/a')
        
        # Pivot column
        pivoted_rows = grouped_rows.pivot_table(index=['Rounded Income', 'City','House Hold Count'], columns='Walking Distance to Bus Stop', values='count', aggfunc='sum').reset_index()
        
        # Rename columns
        pivoted_rows = pivoted_rows.rename(columns={'House Hold Count':'Count','True': 'W/ Bus Access', 'False': 'W/O Bus Access'})
        
        self.hh_inc_hist_df = pivoted_rows

    def hh_inc_pivot(self,aggregators):
        hh_inc_map = self.hh_inc_map_df.copy()
        hh_inc_map['zone'] = hh_inc_map['zone'].astype(int)
    
        # Add 'city_zone' column
        hh_inc_map['city_zone'] = hh_inc_map['City'] + "_" + hh_inc_map['zone'].astype(str)
        
        hh_inc_map.rename(columns={'hh_inc_total':'HH_Inc_Total','household_count':'HH_Count'}, inplace=True)
        # Remove specified columns
        hh_inc_map = hh_inc_map.drop(columns=['City', 'zone', 'age_class', 'HH_Inc_Total', 'Senior HH Income Average'])
        
        # Reorder columns again
        hh_inc_map = hh_inc_map[['city_zone', 'HH Inc Quartile', 'HH_Count']]
        
        # Pivot column
        pivoted_data = hh_inc_map.pivot_table(index='city_zone', columns='HH Inc Quartile', values='HH_Count', aggfunc='sum').reset_index()
        
        # Split 'city_zone' column into 'City' and 'zone'
        pivoted_data[['City', 'zone']] = pivoted_data['city_zone'].str.split('_', expand=True)
        
        # Change column types
        pivoted_data['zone'] = pivoted_data['zone'].astype(int)
        
        return pivoted_data

    def vehicle_ownership(self,aggregators):
        vo = load_h5_table(path,'fare_sensitivity_results_vo')
        grouped_rows = vo.groupby(['City', 'zone']).agg({
        'households': 'sum',
        'vo': 'sum'
        }).reset_index()
        vo = None
        # Merge with hh_inc_pivot
        merged_data = grouped_rows.merge(self.hh_inc_pivot(aggregators), on=['City', 'zone'], how='left')
        
        # Replace null values with 0
        merged_data.fillna(0, inplace=True)
        
        # Add custom columns
        merged_data['3 VO for HH Inc Between 50% and 75%'] = merged_data.apply(
            lambda row: (row['vo'] / row['households']) if (row['3- between 50% and 75%'] + row['4- >75%'] + row['2- between 25% and 50%'] + row['1- <25%']) == 0 else (row['vo'] / row['households']) * row['3- between 50% and 75%'] / (row['3- between 50% and 75%'] + row['4- >75%'] + row['2- between 25% and 50%'] + row['1- <25%']), axis=1)
        
        merged_data['4 VO for HH Inc >75%'] = merged_data.apply(
            lambda row: (row['vo'] / row['households']) if (row['3- between 50% and 75%'] + row['4- >75%'] + row['2- between 25% and 50%'] + row['1- <25%']) == 0 else (row['vo'] / row['households']) * row['4- >75%'] / (row['3- between 50% and 75%'] + row['4- >75%'] + row['2- between 25% and 50%'] + row['1- <25%']), axis=1)
        
        merged_data['2 VO for HH Inc between 25% and 50%'] = merged_data.apply(
            lambda row: (row['vo'] / row['households']) if (row['3- between 50% and 75%'] + row['4- >75%'] + row['2- between 25% and 50%'] + row['1- <25%']) == 0 else (row['vo'] / row['households']) * row['2- between 25% and 50%'] / (row['3- between 50% and 75%'] + row['4- >75%'] + row['2- between 25% and 50%'] + row['1- <25%']), axis=1)
        
        merged_data['1 VO for HH Inc between <25%'] = merged_data.apply(
            lambda row: (row['vo'] / row['households']) if (row['3- between 50% and 75%'] + row['4- >75%'] + row['2- between 25% and 50%'] + row['1- <25%']) == 0 else (row['vo'] / row['households']) * row['1- <25%'] / (row['3- between 50% and 75%'] + row['4- >75%'] + row['2- between 25% and 50%'] + row['1- <25%']), axis=1)
        
        # Change column types
        merged_data = merged_data.astype({
            '1 VO for HH Inc between <25%': 'float64',
            '2 VO for HH Inc between 25% and 50%': 'float64',
            '4 VO for HH Inc >75%': 'float64',
            '3 VO for HH Inc Between 50% and 75%': 'float64'
        })
        
        self.vehicle_ownership_df = merged_data

    

    def run_demographic_queries(self,path,aggregators,db_path,city_names):
        self.load_tables(path)
        self.hh_inc_map(aggregators)
        self.demo_activity_time_usage(aggregators)
        self.demo_agg_tnc_case(aggregators,db_path,city_names)
        self.demo_agg_mode_data(aggregators)
        self.hh_inc_hist(aggregators)
        self.vehicle_ownership(aggregators)

class financial_study():
    def __init__(self,path,aggregators,demographic_study:demographic_queries):
        self.demo_financial_case_data_df = demographic_study.demographic_df
        self.run_financial_queries(path,aggregators)
        

    def load_tables(self,path):
        self.tnc_summary_df = load_h5_table(path,'fare_sensitivity_results')
        self.pooling_rate_df = load_h5_table(path,'tnc_results_discount')
        self.financial_data_df = load_h5_table(path,'requests_sum')
        self.financial_case_data_df = load_h5_table(path,'demo_financial_case_data')
        self.financial_case_data_df['driver pay take rate']=self.financial_case_data_df['total_fare']-self.financial_case_data_df['uber take']
        self.financial_case_data_df_hold = self.financial_case_data_df.copy()

    def agg_demo_financial_data(self,aggregators):
        # Change column types
        dataset = self.financial_case_data_df_hold.copy()
        dataset = dataset.astype({
            'age_class': 'int64',
            'total pay 1_a': 'float64',
            '1_a_cnt_req_under_5': 'int64',
            'total_fare_2_low': 'int64',
            'total_fare_2_high': 'int64',
            'total_operating_cost_2_minibus': 'float64',
            'total_operating_cost_2_van': 'float64',
            'total_operating_cost_3': 'float64',
            'total_fare': 'float64',
            'requests': 'int64',
            'Fleet Size': 'str',
            'revenue per vehicle': 'float64',
            'revenue per request': 'float64',
            'revenue_minutes_traveled': 'float64',
            'revenue_miles_traveled': 'float64',
            'folder': 'str',
            'uber take': 'float64',
            'origin_zone': 'str'
        })
        
        # Filter rows where age_class is 2
        filtered_rows = dataset[dataset['age_class'] == 2]
        
        # Change column type for 'origin_zone'
        filtered_rows['origin_zone'] = filtered_rows['origin_zone'].astype(str)
        filtered_rows.rename(columns={'origin_zone':'zone'},inplace=True)

        return filtered_rows

    def financial_data(self,aggregators):
        dataset = self.financial_case_data_df.copy()
        dataset = dataset.astype({
        'total pay 1_a': 'float64',
        '1_a_cnt_req_under_5': 'int64',
        'total_fare_2_low': 'int64',
        'total_fare_2_high': 'int64',
        'total_operating_cost_2_minibus': 'float64',
        'total_operating_cost_2_van': 'float64',
        'total_operating_cost_3': 'float64',
        'total_fare': 'float64',
        'requests': 'int64',
        'fleet_size': 'str',
        'revenue_miles_traveled': 'float64',
        'revenue_minutes_traveled': 'float64',
        'folder': 'str',
        'uber take': 'float64'
        })

        # Remove specified columns
        dataset = dataset.drop(columns=['revenue per vehicle', 'revenue per request'])

        # Change column type for 'Fleet Size'
        dataset['Fleet Size'] = dataset['fleet_size'].astype(str)

        # Add 'driver pay take rate' column
        dataset['driver pay take rate'] = dataset['total_fare'] - dataset['uber take']

        # Change column type for 'driver pay take rate'
        dataset['driver pay take rate'] = dataset['driver pay take rate'].astype(float)

        # Group rows by specified columns and calculate sum of various columns
        grouped_rows = dataset.groupby(['age_class', 'City', 'Fleet Size', 'Strategy', 'Iteration']).agg({
        'total pay 1_a': 'sum',
        '1_a_cnt_req_under_5': 'sum',
        'total_fare_2_low': 'sum',
        'total_fare_2_high': 'sum',
        'total_operating_cost_2_minibus': 'sum',
        'total_operating_cost_2_van': 'sum',
        'total_operating_cost_3': 'sum',
        'total_fare': 'sum',
        'requests': 'sum',
        'uber take': 'sum',
        'driver pay take rate': 'sum',
        'revenue_minutes_traveled':'sum', 
        'revenue_miles_traveled':'sum'
        }).reset_index()

        self.financial_case_data_df = grouped_rows
        return grouped_rows
        

    def case_1_a(self,aggregators):
        # Remove specified columns
        financial_cases = self.financial_case_data_df.copy()
        financial_cases = financial_cases.drop(columns=[
            'total_fare_2_low', 'total_fare_2_high', 'total_operating_cost_2_minibus', 'total_operating_cost_2_van', 'total_operating_cost_3'
        ])
        
        # Rename 'uber take' column to 'take rate operating revenue'
        financial_cases = financial_cases.rename(columns={'uber take': 'take rate operating revenue'})
        
        # Add 'driver pay operating gross' column
        financial_cases['driver pay operating gross'] = financial_cases['total_fare'] - financial_cases['total pay 1_a']
        
        # Remove 'driver pay take rate' column
        financial_cases = financial_cases.drop(columns=['driver pay take rate'])
        
        # Rename 'driver pay operating gross' to 'Gross Profit'
        financial_cases = financial_cases.rename(columns={'driver pay operating gross': 'Gross Profit'})
        
        # Remove 'take rate operating revenue' column
        financial_cases = financial_cases.drop(columns=['take rate operating revenue'])
        
        return financial_cases
    
    def case_1_b(self,aggregators):
        financial_cases = self.financial_case_data_df.copy()
        financial_cases = financial_cases.rename(columns={'uber take': 'Gross Profit'})
    
    # Remove specified columns
        financial_cases = financial_cases.drop(columns=[
            'total_fare_2_low', 'total_fare_2_high', 'total_operating_cost_2_minibus', 'total_operating_cost_2_van', 'total_operating_cost_3', 'total pay 1_a', '1_a_cnt_req_under_5'
        ])
        
        return financial_cases
    
    def case_2(self,aggregators):
        financial_cases = self.financial_case_data_df.copy()
        financial_cases = financial_cases.drop(columns=[
            '1_a_cnt_req_under_5', 'total_operating_cost_3', 'revenue_minutes_traveled', 'revenue_miles_traveled', 'uber take', 'driver pay take rate'
        ])
        
        # Add 'Low Fare Gross Profit Van' column
        financial_cases['Low Fare Gross Profit Van'] = financial_cases['total_fare_2_low'] - financial_cases['total_operating_cost_2_van']
        
        # Add 'High Fare Gross Profit Van' column
        financial_cases['High Fare Gross Profit Van'] = financial_cases['total_fare_2_high'] - financial_cases['total_operating_cost_2_van']
        
        # Add 'Low Fare Gross Profit Minibus' column
        financial_cases['Low Fare Gross Profit Minibus'] = financial_cases['total_fare_2_low'] - financial_cases['total_operating_cost_2_minibus']
        
        # Add 'High Fare Gross Profit Minibus' column
        financial_cases['High Fare Gross Profit Minibus'] = financial_cases['total_fare_2_high'] - financial_cases['total_operating_cost_2_minibus']
        
        # Change column types
        financial_cases = financial_cases.astype({
            'High Fare Gross Profit Minibus': 'float64',
            'Low Fare Gross Profit Minibus': 'float64',
            'High Fare Gross Profit Van': 'float64',
            'Low Fare Gross Profit Van': 'float64'
        })
        
        return financial_cases
        
    def case_3(self,aggregators):
        financial_cases = self.financial_case_data_df.copy()
        financial_cases = financial_cases.drop(columns=[
            'total_fare_2_low', 'total_fare_2_high', 'total_operating_cost_2_minibus', 'total_operating_cost_2_van', 'uber take', 'driver pay take rate', 'total pay 1_a', '1_a_cnt_req_under_5'
        ])
        
        # Add 'Gross Profit' column
        financial_cases['Gross Profit'] = financial_cases['total_fare'] - financial_cases['total_operating_cost_3']
        
        # Change column types
        financial_cases = financial_cases.astype({
            'Gross Profit': 'float64'
        })
        
        return financial_cases

    def demo_financial_case_data_study(self,aggregators):
        demographic_data = self.demo_financial_case_data_df.copy()
        demographic_data['dist_miles.1'] = demographic_data['AVG_DIST'] * 1609.34 * demographic_data['REQUESTS']
    
        # Group rows by specified columns and calculate sum of 'dist_miles.1' and 'REQUESTS'
        group_cols = [col for col in aggregators['Folder to Columns'] if col is not None] + ['zone']
        grouped_rows = demographic_data.groupby(group_cols).agg({
            'dist_miles.1': 'sum',
            'REQUESTS': 'sum'
        }).reset_index()
        grouped_rows["Fleet Size"] = grouped_rows["Fleet Size"].astype(str)
        grouped_rows["zone"] = grouped_rows["zone"].astype(str)
        # Merge with financial_case_data_summary_elders
        merged_data = grouped_rows.merge( self.agg_demo_financial_data(aggregators), left_on=group_cols, right_on=group_cols, how='left')
        
        # Filter rows where 'total_fare' is not null
        filtered_rows = merged_data[merged_data['total_fare'].notnull()]
        
        # Add 'cost per mile' column
        filtered_rows['cost per mile'] = filtered_rows['total_fare'] / filtered_rows['dist_miles.1']
        
        # Add 'Per Mile Level' column
        filtered_rows['Per Mile Level'] = filtered_rows['cost per mile'].apply(
            lambda x: 1 if x > 1 else (2 if x > 0.8 else (3 if x > 0.7 else (4 if x > 0.6 else 5)))
        )
        self.demo_financial_case_data_df = filtered_rows
        return filtered_rows

    def combined_financial_cases(self,aggregators):
        self.financial_data(aggregators)
        case_1_a = self.case_1_a(aggregators)
        case_1_b = self.case_1_b(aggregators)
        case_2 = self.case_2(aggregators)
        case_3 = self.case_3(aggregators)
        
        merged_data = case_1_a.merge(case_1_b, on=[col for col in aggregators['Folder to Columns'] if col is not None], how='left', suffixes=('', '_Case_1_b'))
        
        # Rename columns
        merged_data = merged_data.rename(columns={'Gross Profit': 'Case 1 A', 'Gross Profit_Case_1_b': 'Case 1 B'})
        
        # Merge with Case 2
        merged_data = merged_data.merge(case_2, on=[col for col in aggregators['Folder to Columns'] if col is not None], how='left')
        
        # Merge with Case 3
        merged_data = merged_data.merge(case_3, on=[col for col in aggregators['Folder to Columns'] if col is not None], how='left', suffixes=('', '_Case_3'))
        
        # Rename columns
        merged_data = merged_data.rename(columns={
            'Low Fare Gross Profit Van': 'Case 2:Low Fare Van',
            'High Fare Gross Profit Van': 'Case 2:High Fare Van',
            'Low Fare Gross Profit Minibus': 'Case 2:Low Fare Minibus',
            'High Fare Gross Profit Minibus': 'Case 2:High Fare Minibus',
            'Gross Profit': 'Case 3'
        })
        
        # Change column types
        merged_data = merged_data.astype({
            'Case 1 A': 'float64',
            'Case 1 B': 'float64',
            'Case 2:Low Fare Van': 'float64',
            'Case 2:High Fare Van': 'float64',
            'Case 2:Low Fare Minibus': 'float64',
            'Case 2:High Fare Minibus': 'float64',
            'Case 3': 'float64'
        })
        
        self.combined_financial_cases_df = merged_data

    def run_financial_queries(self,path,aggregators):
        self.load_tables(path)
        self.combined_financial_cases(aggregators)
        self.demo_financial_case_data_study(aggregators)

def create_h5(df_dict,path):
    with pd.HDFStore(path+'/pbix_tables.h5') as store:
        for key, df in df_dict.items():
            store[key] = df


path = r"C:\Users\jpaul4\Desktop\temp\results.h5"
aggregators = {
    'Transit Aggregators': ['City', 'Strategy'],
    'Folder to Columns': ['City', 'Strategy','Fleet Size'],
    'Demographic Aggregators':['City','age_class'],
    'Case Aggregators': ['City', 'Fleet Size'],
    'Strategy Aggregators': ['City', 'Strategy']
}
db_path = os.path.dirname(path)
city_names = {
    'greenville':'gsc',
    'campo':'atx'
}
queries = transit_queries(path, aggregators)
mode_queries = mode_shift_queries(path,aggregators)
demographic_queries_instance = demographic_queries(path,aggregators,db_path, city_names)
financial_study_instance = financial_study(path,aggregators,demographic_queries_instance)

df_dict = {"trip_avo_histogram_vals":queries.trip_avo_histogram_vals,
"pattern_avo_histogram_vals":queries.pattern_avo_histogram_vals,
"transit_pattern_avo_cases_vals":queries.transit_pattern_avo_cases_vals,
"transit_trip_avo_cases_vals":queries.transit_trip_avo_cases_vals,
"mode_dist_combo_proactive":mode_queries.mode_dist_combo_proactive,
"mode_cnt_combo_proactive":mode_queries.mode_cnt_combo_proactive,
"positive_shift_reg":mode_queries.positive_shift_reg,
"demographic_df":demographic_queries_instance.demographic_df,
"hh_inc_map_df":demographic_queries_instance.hh_inc_map_df,
"hh_inc_map_atx":demographic_queries_instance.hh_inc_map_atx,
"hh_inc_map_gsc":demographic_queries_instance.hh_inc_map_gsc,
"hh_inc_hist_df":demographic_queries_instance.hh_inc_hist_df,
"demo_activity_time_usage_df":demographic_queries_instance.demo_activity_time_usage_df,
"demo_agg_tnc_case_df":demographic_queries_instance.demo_agg_tnc_case_df,
"demo_agg_mode_data_df":demographic_queries_instance.demo_agg_mode_data_df,
"vehicle_ownership_df":demographic_queries_instance.vehicle_ownership_df,
"closest_stops_by_zone_df":demographic_queries_instance.closest_stops_by_zone_df,
"vehicle_ownership_df":demographic_queries_instance.vehicle_ownership_df,
"combined_financial_cases_df":financial_study_instance.combined_financial_cases_df,
"financial_data_df":financial_study_instance.financial_data_df,
"pooling_rate_df":financial_study_instance.pooling_rate_df,
"tnc_summary_df":financial_study_instance.tnc_summary_df,
"financial_case_data_df":financial_study_instance.financial_case_data_df,
"demo_financial_case_data_df":financial_study_instance.demo_financial_case_data_df,
"mode_dist_combo":mode_queries.mode_dist_combo,
"mode_cnt_combo":mode_queries.mode_cnt_combo
}
create_h5(df_dict,db_path)

# print(queries.trip_avo_histogram_vals)
# print(queries.pattern_avo_histogram_vals)
# print(queries.transit_pattern_avo_cases_vals)
# print(queries.transit_trip_avo_cases_vals)
# print(mode_queries.mode_dist_combo_proactive)
# print(mode_queries.mode_cnt_combo_proactive)
# print(mode_queries.positive_shift_reg)
# print(demographic_queries_instance.demographic_df)
# print(demographic_queries_instance.hh_inc_map_df)
# print(demographic_queries_instance.hh_inc_map_atx)
# print(demographic_queries_instance.hh_inc_map_gsc)
# print(demographic_queries_instance.hh_inc_hist_df)
# print(demographic_queries_instance.demo_activity_time_usage_df)
# print(demographic_queries_instance.demo_agg_tnc_case_df)
# print(demographic_queries_instance.demo_agg_mode_data_df)
# print(demographic_queries_instance.vehicle_ownership_df)
# print(demographic_queries_instance.closest_stops_by_zone_df)
# print(demographic_queries_instance.vehicle_ownership_df)
# print(financial_study_instance.combined_financial_cases_df)
# print(financial_study_instance.financial_data_df)
# print(financial_study_instance.pooling_rate_df)
# print(financial_study_instance.tnc_summary_df)
# print(financial_study_instance.financial_case_data_df)
