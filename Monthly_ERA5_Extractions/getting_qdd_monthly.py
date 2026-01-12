import os
import xarray as xr
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import time
import psutil
import sys

sys.stdout = open(sys.stdout.fileno(), mode='w', buffering=1)

# Defining functions
def calculate_qdd(t2_max, t2_mean, t2_min, t_base=22):
    """Calculate cooling degree days (CDD) based on temperature thresholds."""
    cdd = np.zeros_like(t2_max)
    condition1 = t2_max <= t_base
    condition2 = (t2_mean <= t_base) & (t_base < t2_max)
    condition3 = (t2_min < t_base) & (t_base < t2_mean)
    condition4 = t2_min >= t_base

    cdd[condition1] = 0
    cdd[condition2] = (t2_max[condition2] - t_base) / 4
    cdd[condition3] = (t2_max[condition3] - t_base) / 2 - (t_base - t2_min[condition3]) / 4
    cdd[condition4] = t2_mean[condition4] - t_base

    return cdd

def convert_longitudes(lon):
    """Convert longitudes from 0-360 to -180-180."""
    return (lon + 180) % 360 - 180


def monitor_memory(step=""):
    """Prints the current memory usage."""
    process = psutil.Process(os.getpid())
    print(f"Memory usage after {step}: {process.memory_info().rss / 1024 ** 2:.2f} MB")


# Path to the folder containing the NetCDF files
data_path = '/dx03/data/cockburn_era5/daily_data'
# Path to the shapefile containing country boundaries
shapefile_path = '/home/ccockburn/natural_earth_shapefiles/ne_110m_admin_0_countries.shp'
# Load the shapefile using GeoPandas
countries = gpd.read_file(shapefile_path)
countries = countries.rename(columns={"ADMIN": "Country"})

# Loop through the years and months
for year in range(2000, 2025):
    # Initialize an empty list to store results for the current year
    year_results = []
    
    for month in range(1, 13):
        print(year, ':', month)
        start_time = time.time()
        file_name = f"era5_daily_q_{year}_{month:02d}.nc"
        file_path = os.path.join(data_path, file_name)

        if os.path.exists(file_path):
            # Open the NetCDF file
            ds = xr.open_dataset(file_path)

            # Extract the temperature variables
            Q_mean = ds['Q_mean'].values
            Q_min = ds['Q_min'].values
            Q_max = ds['Q_max'].values


            qdd_daily = calculate_qdd(Q_max, Q_mean, Q_min, t_base = 22)

            # Extract the latitude and longitude values
            lat_values = ds['latitude'].values
            lon_values = ds['longitude'].values
            lon_values = convert_longitudes(lon_values)

            # Convert the results to a DataFrame
            month_results = []
            days_count = Q_mean.shape[0]
            for lat_index in range(len(lat_values)):
                for lon_index in range(len(lon_values)):
                    month_results.append({
                        'Date': pd.Timestamp(year=year, month=month, day=1),
                        'lat': lat_values[lat_index],
                        'lon': lon_values[lon_index],
                        'avg_Q': Q_mean[:, lat_index, lon_index].mean(),
                        'qdd': qdd_daily[:, lat_index, lon_index].sum(),
                    })

            df = pd.DataFrame(month_results)

            # Create a GeoDataFrame from the DataFrame
            geometry = [Point(xy) for xy in zip(df['lon'], df['lat'])]
            gdf = gpd.GeoDataFrame(df, geometry=geometry)
            gdf = gdf.set_crs(countries.crs, allow_override=True)
            gdf_joined = gpd.sjoin(gdf, countries, how='left', op='intersects')

            # Monitor memory usage
            monitor_memory(step=f"processing {year}-{month:02d}")

            # Group by Date and country, and calculate the averages and sums
            country_cdd_avg = gdf_joined.groupby(['Date', 'Country'])['qdd'].mean().reset_index()
            country_cdd_sum = gdf_joined.groupby(['Date', 'Country'])['qdd'].sum().reset_index()
            country_avg_temp = gdf_joined.groupby(['Date', 'Country'])[['avg_Q']].mean().reset_index()


            # Merge the results
            country_results = pd.merge(country_cdd_avg, country_cdd_sum, on=['Date', 'Country'],
                                       suffixes=('_avg', '_sum'))
            country_results = pd.merge(country_results, country_avg_temp, on=['Date', 'Country'])

            # Append the results to the list for the current year
            year_results.append(country_results)

            print(year, ':', month, ' (', time.time() - start_time, ')')

    # Concatenate all the results for the current year
    final_year_results = pd.concat(year_results, ignore_index=True)

    # Save the final results for the current year to a CSV file
    final_year_results.to_csv(f'/dx03/data/cockburn_era5/monthly_qdd_{year}.csv', index=False)

    print(f'saved results for {year}!')

print('All files saved!')
