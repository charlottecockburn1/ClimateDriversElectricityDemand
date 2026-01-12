import xarray as xr
import os
import numpy as np
import re
import time
import sys

sys.stdout = open(sys.stdout.fileno(), mode='w', buffering=1)

def calculate_vapor_pressure(T):
    """Saturation vapor pressure (hPa) using temperature in Celsius."""
    return 6.112 * np.exp((17.62 * T) / (243.12 + T))

def calculate_specific_humidity_ratio(Td, P=1013.25):
    """Compute humidity ratio W (kg/kg dry air) from dew point and pressure."""
    e = calculate_vapor_pressure(Td)  # actual vapor pressure from dew point
    return 0.622 * (e / (P - e))

def calculate_enthalpy(T, W):
    """Calculate moist air enthalpy (kJ/kg dry air)."""
    return 1.006 * T + W * (2501 + 1.86 * T)

# Constants
W_ref = 0.0116  # Reference humidity ratio for Qb
T_threshold = 25.6  # °C

# Paths
data_folder = "/dx03/data/cockburn_era5/downloads"
output_folder = "/dx03/data/cockburn_era5/daily_data"

years = [str(y) for y in range(2000, 2025)]

for file in sorted(os.listdir(data_folder)):
    if file.endswith(".nc") and "2d" in file and any(year in file for year in years):
        start_time = time.time()

        file_path = os.path.join(data_folder, file)
        temp_file = file.replace("2d", "2t").replace("168", "167")
        temp_file_path = os.path.join(data_folder, temp_file)

        print(f"Processing {file}...")

        # Load datasets with Dask for memory efficiency
        ds_dew = xr.open_dataset(file_path, chunks={"time": "auto"})
        ds_temp = xr.open_dataset(temp_file_path, chunks={"time": "auto"})

        # Convert Kelvin to Celsius
        Td = ds_dew["VAR_2D"] - 273.15
        T = ds_temp["VAR_2T"] - 273.15

        # Calculate actual enthalpy Q from T and dew point
        W = calculate_specific_humidity_ratio(Td)
        Q = calculate_enthalpy(T, W)

        # Daily means of Q and Qb
        Q_mean = Q.resample(time="1D").mean()
        Q_min = Q.resample(time="1D").min()
        Q_max = Q.resample(time="1D").max()

        # Assemble output dataset
        ds_daily = xr.Dataset({
            "Q_mean": Q_mean.compute(),
            "Q_min": Q_min.compute(),
            "Q_max": Q_max.compute(),
        })

        # Output filename
        match = re.search(r'(\d{4})(\d{2})\d{2}', file)
        year, month = match.groups()
        new_filename = f"era5_daily_q_{year}_{month}.nc"
        output_file = os.path.join(output_folder, new_filename)

        # Save to NetCDF
        ds_daily.to_netcdf(output_file, engine="netcdf4", compute=False)

        print(f"Processed and saved in {time.time() - start_time:.2f} seconds: {output_file}")
