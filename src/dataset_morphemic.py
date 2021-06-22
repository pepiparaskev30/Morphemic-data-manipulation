import os
import glob
import pandas as pd
from pathlib import Path
import numpy as np
import csv
import datetime

features = []
list_with_csv_files = []
list_ = []
list_for_index = []


# CHECK THE CODE FOR EACH DATA DIRECTORY

os.chdir(
    "/Users/pepiparaskevoulakou/Desktop/Melodic_1/time-series-data/time-series-data/genome/deployment-reconfiguration-range-4-to-4/2021-02-18 to 2021-02-18/v1.0-raw data"
)

dataset_folder = "."

files = [
    "./EstimatedRemainingTimeContext.csv",
    "./SimulationLeftNumber.csv",
    "./SimulationElapsedTime.csv",
    "./NotFinishedOnTime.csv",
    "./MinimumCoresContext.csv",
    "./NotFinished.csv",
    "./WillFinishTooSoonContext.csv",
    "./NotFinishedOnTimeContext.csv",
    "./MinimumCores.csv",
    "./ETPercentile.csv",
    "./RemainingSimulationTimeMetric.csv",
    "./TotalCores.csv",
]


def get_all_files():
    _files = []
    for root, dirs, files in os.walk(dataset_folder):
        for filename in files:
            if not filename.endswith(".DS_Store"):
                _files.append(root + "/" + filename)
                features.append(filename[:-4])
    # print(_files)
    # print(features)
    return _files, features


def create_df():
    df = pd.DataFrame(columns=features_list)
    # print(df)
    return df


def create_the_list_of_csv_files():
    for _file in files:
        list_with_csv_files.append(_file[2:])
    # print(list_with_csv_files)
    return list_with_csv_files


features_list = create_the_list_of_csv_files()
# print(features_list)


def append_all_rows():  ##appends all rows from each csv in a list
    for each_file in features_list:
        with open(each_file, "r") as file:
            reader = csv.reader(file)
            for row in reader:
                list_.append(row)
    return list_


_final_list = append_all_rows()
# print(_final_list)


def insert_rows_to_df():  # create dictionaries for each element in the previous list and appends it to a df
    df_unified = create_df()
    data = []
    for _l in range(2, len(_final_list)):
        _name, _time, _value = (
            _final_list[_l][0],
            _final_list[_l][1],
            _final_list[_l][6],
        )
        empty_dict = {}
        values = [_value, _time]
        _keys = [_name, "Datetime"]
        zipped = zip(_keys, values)
        emptyList = dict(zipped)
        data.append(emptyList)
    for counter in range(len(data)):
        df_unified = df_unified.append(data[counter], True)
        df_unified = df_unified[df_unified.Datetime != "time"]
    return df_unified


### dataframe creation based on the above functions
_files, features_list = get_all_files()
features_list.append("Datetime")  ## append a new column for capturing Datetime
features_list = (
    features_list[-1:] + features_list[:-1]
)  # bring the "Datetime" column as the first column
features_list.remove("descrip")  # comment it if not exists


def convert_dataframe_time(a_df):
    for c_ in range(len(a_df["Datetime"])):
        a_df["Datetime"][c_] = (
            datetime.datetime.strptime(my_df["Datetime"][c_], "%Y-%m-%dT%H:%M:%SZ")
            if a_df["Datetime"][c_][-4:-3] == ":"
            else datetime.datetime.strptime(
                my_df["Datetime"][c_], "%Y-%m-%dT%H:%M:%S.%fZ"
            )
        )
        # a_df['Datetime'][c_] = datetime.datetime.strptime(my_df['Datetime'][c_], '%Y-%m-%dT%H:%M:%S.%fZ')
        a_df["Datetime"][c_] = int(my_df["Datetime"][c_].timestamp())
    return a_df


def sortdfRows(a_df):
    new_df = a_df.sort_values(
        by=["Datetime"], inplace=True
    )  # be careful !a new variable for dataframe must be assigned
    return new_df


def aggregation_based_timestamp():
    features_ = [
        "Datetime",
        "RemainingSimulationTimeMetric",
        "WillFinishTooSoonContext",
        "ETPercentile",
        "SimulationLeftNumber",
        "MinimumCoresContext",
        "SimulationElapsedTime",
        "NotFinishedOnTime",
        "MinimumCores",
        "TotalCores",
        "NotFinishedOnTimeContext",
        "NotFinished",
        "EstimatedRemainingTimeContext",
    ]
    value_list = []
    for i in range(len(features_)):
        value_list.append("first")
    aggregation_functions = dict(zip(features_, value_list))
    return aggregation_functions
    a_new = a_df.groupby(a_df["Datetime"]).aggregate(aggregation_functions)


# def save_as_csv(a_df):
# return(a_df.to_csv(r'/Users/pepiparaskevoulakou/Desktop/Melodic_1/dataset_3.csv')) #save the df in a csv file

# apply the functions and add new modifications into the dataset
my_df = create_df()
my_df = insert_rows_to_df()
my_df = my_df[my_df.Datetime != "time"]
my_df = convert_dataframe_time(my_df)
my_df.sort_values(by=["Datetime"], inplace=True)  # sort rows by Datetime column in Unix

"""merge rows by duplicate Datetime values and apply the proper values from each column value"""
my_df = my_df.groupby(my_df["Datetime"]).aggregate(aggregation_based_timestamp())

# save the entire modifications as a unified csv
my_df.to_csv(
    r"/Users/pepiparaskevoulakou/Desktop/Melodic_1/dataset_repo/new_18_02_2021_range_4_to_4.csv"
)  # save the df in a csv file
