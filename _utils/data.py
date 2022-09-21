from _utils.typing import PathLike
from _utils.time import parse_timestr

import pandas as pd
import datetime as dt
import pickle

def cense_data(path: PathLike, output: PathLike) -> None:
    """
    Complete a data census, counting how many data points are in the premarket,
    atmarket, and postmarket for each day in a data set. Will produce a csv file
    with the following format:
    
    date       , premarket data points , atmarket data points , postmarket data points
    2019-01-02 ,                  1120 ,                23400 ,                     64
    ...
    2022-01-02 ,                  1430 ,                23400 ,                    190

    Args:
        path (PathLike): Path to DataFrame pickle.
        output (PathLike): Path to output file.
    """
    
    census = ["date,premarket,atmarket,postmarket,total"]

    df: pd.DataFrame = pd.read_pickle(path)
    df = df.sort_values("time")

    df = df.values.tolist()

        
    curr_date: dt.date = None
    day_data = [[], [], []]

    day_data_count = 0
    for row in df:
        time = row[0]

        # time: dt.datetime = parse_timestr(time)
        time, date = time.time(), time.date()

        if curr_date is None:
            curr_date = date

        if date != curr_date:
            census.append(f"{curr_date},{len(day_data[0])},{len(day_data[1])},{len(day_data[2])},{day_data_count}")

            # uncomment three lines to separate otherwise it is just a read
            # if day_data != []:
            #     with open(f"{root}/{curr_date}.pckl", "wb") as f:
            #         pickle.dump(day_data, f)

            curr_date = date
            day_data = [[], [], []]
            day_data_count = 0
        else:
            if time < dt.time(9, 30):
                index = 0
            elif time > dt.time(16):
                index = 2
            else:
                index = 1

            day_data[index].append(row)
            day_data_count += 1
    
    with open(output, "a") as f:
        for line in census:
            f.write(f"{line}\n")
            

def split_data(path: PathLike, root: PathLike, census_output_dir: PathLike) -> None:
    """
    Split data into days and days into premarket, atmarket, and postmarket data points.
    Different files will be created in the specified directory.
    
    Will also complete a data census, counting how many data points are in the premarket,
    atmarket, and postmarket for each day in a data set. Will produce a csv file
    with the following format:
    
    date       , premarket data points , atmarket data points , postmarket data points
    2019-01-02 ,                  1120 ,                23400 ,                     64
    ...
    2022-01-02 ,                  1430 ,                23400 ,                    190

    Args:
        path (PathLike): Path to DataFrame pickle.
        root (PathLike): Path to output directory.
        census_output_dir (PathLike): Path to where the census is to be outputed to.
    """
    
    census = ["date,premarket,atmarket,postmarket,total"]

    df: pd.DataFrame = pd.read_pickle(path)
    df = df.sort_values("time")

    df = df.values.tolist()

        
    curr_date: dt.date = None
    day_data = [[], [], []]

    day_data_count = 0
    for row in df:
        time = row[0]

        # time: dt.datetime = parse_timestr(time)
        time, date = time.time(), time.date()

        if curr_date is None:
            curr_date = date

        if date != curr_date:
            census.append(f"{curr_date},{len(day_data[0])},{len(day_data[1])},{len(day_data[2])},{day_data_count}")

            if day_data != []:
                with open(f"{root}/{curr_date}.pckl", "wb") as f:
                    pickle.dump(day_data, f)

            curr_date = date
            day_data = [[], [], []]
            day_data_count = 0
        else:
            if time < dt.time(9, 30):
                index = 0
            elif time > dt.time(16):
                index = 2
            else:
                index = 1

            day_data[index].append(row)
            day_data_count += 1

    census.append(f"{curr_date},{len(day_data[0])},{len(day_data[1])},{len(day_data[2])},{day_data_count}")
    if day_data != []:
        with open(f"{root}/{curr_date}.pckl", "wb") as f:
            pickle.dump(day_data, f)
    
    with open(census_output_dir, "a") as f:
        for line in census:
            f.write(f"{line}\n")