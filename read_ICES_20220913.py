"""
This code processes ICES (International Council for the Exploration of the Sea) data files, extracting relevant 
information and saving it as NetCDF files. The script handles different file types including bot, ctd, and xbt,
reading data in chunks, extracting data into structured lists, and then organizing the data into an Xarray Dataset 
before saving it.

Steps followed in the script:
1. Initialize the ICESReader class.
2. Read and process chunks of data from large text or CSV files.
3. Organize the data into an Xarray Dataset.
4. Save the Dataset as a NetCDF file.

Functions and methods in the script:
1. ICESReader.initialize_variables: Initializes lists and variables for data storage.
2. ICESReader.get_date: Parses date components into datetime objects and timestamps.
3. ICESReader.process_chunks: Processes chunks of data from the text or CSV files.
4. ICESReader.create_dataset: Creates an Xarray Dataset from the processed data.
5. ICESReader.run: Executes the data processing workflow.
6. main: Main function to run the ICESReader for multiple data files.
"""

import os
from datetime import datetime
from tqdm import tqdm
import xarray as xr
import pandas as pd
import numpy as np


class ICESReader:
    def __init__(self):
        """Initializes the ICESReader class."""
        pass

    def initialize_variables(self, file_type):
        """
        Initializes lists and variables for data storage based on the file type.

        Parameters:
        - file_type: Type of the file (e.g., 'bot', 'ctd', 'xbt').

        Returns:
        - string_attrs: List of string attributes to be extracted.
        - groupby_attrs: List of attributes to group the data by.
        - measurements_attrs: List of measurement attributes to be extracted.
        - data_lists: Dictionary to store extracted data.
        """
        if file_type != "xbt":
            string_attrs = [
                "orig_cruise_id",
                "station_no",
                "lat",
                "lon",
                "datestr",
                "timestamp",
                "bottom_depth",
                "shallowest_depth",
                "deepest_depth",
                "parent_index",
            ]
            groupby_attrs = [
                "Cruise",
                "Station",
                "Year",
                "Month",
                "Day",
                "Hour",
                "Minute",
                "Longitude [degrees_east]",
                "Latitude [degrees_north]",
                "Bot. Depth [m]",
            ]
            measurements_attrs = ["depth", "press", "temp", "psal"]
        else:
            string_attrs = [
                "orig_cruise_id",
                "station_no",
                "lat",
                "lon",
                "datestr",
                "timestamp",
                "shallowest_depth",
                "deepest_depth",
                "parent_index",
            ]
            groupby_attrs = [
                "Cruise",
                "Station",
                "Year",
                "Month",
                "Day",
                "Hour",
                "Minute",
                "Longitude [degrees_east]",
                "Latitude [degrees_north]",
            ]
            measurements_attrs = ["depth", "temp"]
        data_lists = {attr: [] for attr in string_attrs + measurements_attrs}
        return string_attrs, groupby_attrs, measurements_attrs, data_lists

    def get_date(self, year, month, day, hour, minute):
        """
        Parses date components into datetime objects and timestamps.

        Parameters:
        - year: Year component of the date.
        - month: Month component of the date.
        - day: Day component of the date.
        - hour: Hour component of the date.
        - minute: Minute component of the date.

        Returns:
        - datestr: Formatted date string.
        - timestamp: Unix timestamp of the date.
        """
        datestr = datetime(int(year), int(month), int(day), int(hour), int(minute))
        timestamp = datestr.timestamp()
        datestr = datetime.strftime(datestr, "%Y/%m/%d %H:%M:%S")

        return datestr, timestamp

    def process_chunks(self, reader, data_lists, file_type, groupby_attrs):
        """
        Processes chunks of data from the text or CSV files.

        Parameters:
        - reader: Pandas reader object to read chunks of the file.
        - data_lists: Dictionary to store extracted data.
        - file_type: Type of the file (e.g., 'bot', 'ctd', 'xbt').
        - groupby_attrs: List of attributes to group the data by.
        """
        i = 0
        for chunk in reader:
            grouped_df = chunk.groupby(groupby_attrs)
            for group, data in grouped_df:
                if file_type != "xbt":
                    orig_cruise_id, station, year, month, day, hour, minute, lon, lat, bottom_depth = group
                    data_lists["bottom_depth"].append(bottom_depth)
                else:
                    orig_cruise_id, station, year, month, day, hour, minute, lon, lat = group
                data_lists["orig_cruise_id"].append(orig_cruise_id)
                data_lists["station_no"].append(station)
                data_lists["lat"].append(lat)
                data_lists["lon"].append(lon)
                datestr, timestamp = self.get_date(year, month, day, hour, minute)
                data_lists["datestr"].append(datestr)
                data_lists["timestamp"].append(timestamp)

                data_lists["depth"].extend(data["Depth [m]"])
                data_lists["temp"].extend(data["Temperature [degC]"])
                if file_type != "xbt":
                    data_lists["press"].extend(data["Pressure [dbar]"])
                    data_lists["psal"].extend(data["Practical Salinity [dmnless]"])

                if len(data["Depth [m]"]) > 1:
                    data_lists["shallowest_depth"].append(min(data["Depth [m]"][data["Depth [m]"] != 0]))
                else:
                    data_lists["shallowest_depth"].append(min(data["Depth [m]"]))
                data_lists["deepest_depth"].append(max(data["Depth [m]"]))
                data_lists["parent_index"].extend([i] * len(data["Depth [m]"]))
                i += 1

    def create_dataset(self, data_lists, string_attrs, measurements_attrs, data_path, save_path):
        """
        Creates an Xarray Dataset from the processed data.

        Parameters:
        - data_lists: Dictionary containing lists of data for each attribute.
        - string_attrs: List of string attributes to include in the Dataset.
        - measurements_attrs: List of measurement attributes to include in the Dataset.
        - data_path: Path to the original data file.
        - save_path: Path where the processed NetCDF files will be saved.

        The method saves the Dataset as a NetCDF file in the specified save path.
        """
        if not os.path.isdir(save_path):
            os.mkdir(save_path)
        os.chdir(save_path)
        ds = xr.Dataset(
            coords=dict(
                timestamp=(["profile"], data_lists["timestamp"]),
                lat=(["profile"], data_lists["lat"]),
                lon=(["profile"], data_lists["lon"]),
            ),
            data_vars=dict(
                **{
                    attr: xr.DataArray(data_lists[attr], dims=["profile"])
                    for attr in string_attrs
                    if attr not in ["lat", "lon", "timestamp", "parent_index"]
                },
                **{attr: xr.DataArray(data_lists[attr], dims=["obs"]) for attr in measurements_attrs},
                parent_index=xr.DataArray(data_lists["parent_index"], dims=["obs"]),
            ),
            attrs=dict(
                dataset_name="ICES_2022",
                creation_date=str(datetime.now().strftime("%Y-%m-%d %H:%M")),
            ),
        )

        file_name = data_path.split("/")[-2]
        ds.to_netcdf(f"{file_name}_raw.nc")

    def run(self, data_path, save_path, file_type):
        """
        Executes the data processing workflow.
        Initializes variables, reads data, processes chunks, and creates the Dataset.

        Parameters:
        - data_path: Path to the data file.
        - save_path: Path where the processed NetCDF files will be saved.
        - file_type: Type of the file (e.g., 'bot', 'ctd', 'xbt').
        """
        string_attrs, groupby_attrs, measurements_attrs, data_lists = self.initialize_variables(file_type)

        if data_path.endswith(".txt"):
            with pd.read_csv(data_path, chunksize=10**6, low_memory=False, sep="\t") as reader:
                self.process_chunks(reader, data_lists, file_type, groupby_attrs)
                self.create_dataset(data_lists, string_attrs, measurements_attrs, data_path, save_path)
        elif data_path.endswith(".csv"):
            with pd.read_csv(data_path, chunksize=10**6, low_memory=False) as reader:
                self.process_chunks(reader, data_lists, file_type, groupby_attrs)
                self.create_dataset(data_lists, string_attrs, measurements_attrs, data_path, save_path)


def main():
    """
    Main function to run the ICESReader for multiple data files.

    It defines the paths to the data files, their corresponding file types, and the save path.
    It then initializes an ICESReader object and processes each data file.
    """
    ices_reader = ICESReader()
    data_paths = [
        "/mnt/storage6/caio/AW_CAA/CTD_DATA/ICES_2022/original_data/ICESData_Bottle_to_2022/01110af6-4732-41d1-b272-f538e60d49c0_trim.csv",
        "/mnt/storage6/caio/AW_CAA/CTD_DATA/ICES_2022/original_data/ICESData_CTD_to_2022/027054ba-3719-449b-ae1f-b1d1b27959b1.txt",
        "/mnt/storage6/caio/AW_CAA/CTD_DATA/ICES_2022/original_data/ICESData_XBT_to_2022/ae6db793-7fa6-4eb8-a860-cc4724c8068d.txt",
    ]
    file_types = ["bot", "ctd", "xbt"]
    save_path = "/mnt/storage6/caio/AW_CAA/CTD_DATA/ICES_2022/ncfiles_raw"
    for data_path, file_type in zip(data_paths, file_types):
        ices_reader.run(data_path, save_path, file_type)


if __name__ == "__main__":
    main()
