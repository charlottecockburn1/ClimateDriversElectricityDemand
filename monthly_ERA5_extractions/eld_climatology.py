import os
import xarray as xr
import numpy as np
import pandas as pd
import sys
import time

sys.stdout = open(sys.stdout.fileno(), mode='w', buffering=1)


# -------------------------
# CONFIG
# -------------------------
data_path = '/dx03/data/cockburn_era5/daily_data'
years = range(2001, 2024)

# -------------------------
# Helper: assign seasons
# -------------------------
def assign_season(ds):
    """Add a 'season' coordinate: DJF, MAM, JJA, SON."""
    month = ds['time'].dt.month

    season = xr.full_like(month, "", dtype=object)

    season = xr.where(month.isin([12,1,2]), "DJF", season)
    season = xr.where(month.isin([3,4,5]), "MAM", season)
    season = xr.where(month.isin([6,7,8]), "JJA", season)
    season = xr.where(month.isin([9,10,11]), "SON", season)

    ds = ds.assign_coords(season=("time", season.values))
    return ds


# -------------------------
# LOAD ALL YEARS
# -------------------------
all_datasets = []

for year in years:
    start_time = time.time()
    for month in range(1, 13):
        # Filename pattern
        file_name = f"era5_daily_eld_{year}_{month:02d}.nc"
        file_path = os.path.join(data_path, file_name)

        if not os.path.exists(file_path):
            continue

        ds = xr.open_dataset(file_path)

        # Expect ds to contain ELD, Q, maybe Qb (ignored here)
        if "time" not in ds:
            raise ValueError(f"{file_path} has no time dimension")

        all_datasets.append(ds)

    print(f'Finished opening {year}, took {round(time.time() - start_time)} seconds!')

print(f'Finished opening all years')

# Concatenate all days across all years
full = xr.concat(all_datasets, dim="time")

print('Concatenated!')

# Fix longitudes to -180–180 if needed
if full.longitude.max() > 180:
    full = full.assign_coords(longitude=((full.longitude + 180) % 360 - 180))

print('Assigned lon')
# Assign seasons
full = assign_season(full)

print('Assigned season')
# -------------------------
# COMPUTE CLIMATOLOGY
# -------------------------
climatology = full.groupby("season").mean("time")

print('Got climatology!')

# Keep only seasons in correct order
climatology = climatology.sel(season=["DJF", "MAM", "JJA", "SON"])

# -------------------------
# SAVE OUTPUT
# -------------------------
output_file = "/dx03/data/cockburn_era5/climatologies.nc"
climatology.to_netcdf(output_file)

print(f"Saved seasonal climatology → {output_file}")
