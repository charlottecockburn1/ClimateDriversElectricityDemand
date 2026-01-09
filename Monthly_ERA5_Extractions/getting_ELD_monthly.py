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
for year in range(2000, 2010):
    # Initialize an empty list to store results for the current year
    year_results = []
    
    for month in range(1, 13):
        print(year, ':', month)
        start_time = time.time()
        file_name = f"era5_daily_eld_{year}_{month:02d}.nc"
        file_path = os.path.join(data_path, file_name)

        if os.path.exists(file_path):
            # Open the NetCDF file
            ds = xr.open_dataset(file_path)

            # Extract the temperature variables
            ELD_mean = ds['ELD'].values
            Q_mean = ds['Q_mean'].values
            Qb_mean = ds['Qb_mean'].values

            # Extract the latitude and longitude values
            lat_values = ds['latitude'].values
            lon_values = ds['longitude'].values
            lon_values = convert_longitudes(lon_values)

            # Convert the results to a DataFrame
            month_results = []
            days_count = ELD_mean.shape[0]
            for lat_index in range(len(lat_values)):
                for lon_index in range(len(lon_values)):
                    month_results.append({
                        'Date': pd.Timestamp(year=year, month=month, day=1),
                        'lat': lat_values[lat_index],
                        'lon': lon_values[lon_index],
                        'avg_ELD': ELD_mean[:, lat_index, lon_index].mean(),
                        'avg_Q': Q_mean[:, lat_index, lon_index].mean(),
                        'avg_Qb': Qb_mean[:, lat_index, lon_index].mean()
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
            country_avg_temp = gdf_joined.groupby(['Date', 'Country'])[['avg_ELD', 'avg_Q', 'avg_Qb']].mean().reset_index()

            # Merge the results
            country_results = country_avg_temp

            # Append the results to the list for the current year
            year_results.append(country_results)

            print(year, ':', month, ' (', time.time() - start_time, ')')

    # Concatenate all the results for the current year
    final_year_results = pd.concat(year_results, ignore_index=True)

    # Save the final results for the current year to a CSV file
    final_year_results.to_csv(f'/dx03/data/cockburn_era5/monthly_ELD_{year}.csv', index=False)

    print(f'saved results for {year}!')

print('All files saved!')
