# [WIP] Introduction

Welcome! This is a (unfinished!) personal data project analyzing auto accidents in New York City from 2013-2023. The final results of this project have been published to my [Tableau Public profile.](https://public.tableau.com/app/profile/jeremymenes/viz/NYCCollisions_17336129497660/Dashboard2?publish=yes)

The primary purpose of this repository is to showcase my skills with data handling, cleaning, and analysis using SQL and Python. Secondly this will also serve as documentation of my changes to the raw dataset, and the methodologies I used to clean and manipulate the data.
  
## Project Overview

**Data Source:**  
- The primary dataset for this project was obtained from the [City of New York OpenData website](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95/about_data)  
- Weather data for NYC was obtained from the [National Oceanic and Atmospheric Administration (NOAA)](https://www.ncdc.noaa.gov/cdo-web/datasets)

**Data Size/Scope:**  
- The NYC Motor Collisions dataset contains over 2 million rows, each representing individual auto accidents across the five boroughs of NYC over a ten-year period.  
- The NOAA dataset contains over 300,000 rows of daily weather readings, from 220 individual weather stations in the greater NYC area.

**SQL for Data Cleaning:**  
- I wrote a series of custom SQL queries (available in this repo) to clean, standardize, and correct data inconsistencies directly within the database environment. 

**Python for Integration and Analysis:**  
- To explore the impact of weather conditions on auto accidents, I used Python to combine the two datasets. This involved a complex Python script that matched each auto accident to the closest NOAA weather station and added the recorded weather data to the original Collisions dataset.

**Database Management:**  
- I began this project using a local Microsoft SQL Server instanc. As the project grew in complexity, I migrated to a Docker MySQL server. The MySQL server is now accessed and queried through PyCharm, with integrated Python scripts for analysis.


## Project Highlights

### Expanding the Project with Weather Data from the NOAA

I started this project by analyzing trends solely within the NYC Motor Collisions dataset. After I noticed certain trends in auto accidents during the holiday months, I began to explore how weather conditions may have affected these seasonal trends. 

Fortunatley, public NYC weather data was handily available from the NOAA. The next step would be planning my methodology on how to combine the two datasets. The NOAA data contained various weather readings from many different weather stations sprawled out across NYC’s limits. The issue was more complicated than simply “What were the weather conditions in NYC on the date of the accident?” From its furthest points, NYC is 35 miles long and encompasses over 300 square miles. I would need to take the location of each accident into account, and compare it to localized weather readings from nearby weather stations.

The planned method to combine the two datasets would follow this general logic:
	1 – From the NYC Motor Collisions dataset, obtain the Collision ID, Latitude, Longitude, and Crash Date of each row. (representing a single auto accident event, what date it occured, and the geospacial coordinates of the accident)
	2 – For each accident, return a list of all weather stations from the NOAA dataset that have weather readings for the date of the accident. (along with the geopspacial coordinates of the weather stations, which was thankfully also included in the NOAA dataset)
	3 – Based on the coordinates of the auto accident, return the closest weather station.
	4 – Append the available weather data from the closest weather station to the row in the NYC Collisions dataset, and repeat for each row.

However, I ran into an obstacle with the NOAA database - many of the smaller weather stations had massive holes in their historical data. If I simply added weather data to the NYC dataset from the closest weather station, I would run into situations where data might not be available from the smaller stations. To fix this, I wrote the following Python script to return a list of weather stations whose databases were at least 95% complete. 
  
<details>
<summary>📜 Python Script – Filter for Weather Stations with Complete Data</summary>

```python
import pandas as pd

with open(r"[NOAA_WeatherData.csv", encoding='utf-8-sig') as Weather_table:
    Stations = pd.read_csv(Weather_table)

# Set completeness threshold
completeness_threshold = 0.95
required_columns = ['PRCP', 'TMAX', 'TMIN', 'SNOW']

# Group by station
station_groups = Stations.groupby('NAME')
qualified_stations = []

for station, group in station_groups:
    total_records = len(group)
    complete_records = group[required_columns].dropna().shape[0]
    completeness_ratio = complete_records / total_records if total_records > 0 else 0
    if completeness_ratio >= completeness_threshold:
        qualified_stations.append((station, completeness_ratio))

# Print results
result_df = pd.DataFrame(qualified_stations, columns=['NAME', 'Completeness'])
print(result_df.sort_values(by='Completeness', ascending=False))
```

</details>

This returned the following list of weather stations, which unsurprisingly contained the weather stations from NYC’s 3 major airports, and the Central Park weather station.

| NAME | Completeness |
| --- | --- |
| JFK INTERNATIONAL AIRPORT, NY US | 1.000000 |
| NEWARK LIBERTY INTERNATIONAL AIRPORT, NJ US | 1.000000 |
| LAGUARDIA AIRPORT, NY US | 1.000000 |
| NY CITY CENTRAL PARK, NY US | 1.000000 |
| LONG BRANCH OAKHURST, NJ US | 0.986291 |
| CENTERPORT, NY US | 0.982268 |
| BOONTON 1 SE, NJ US | 0.977084 |

I then filtered the NOAA weather data to only include data from these stations. This ensured that when I would later match each auto accident to the closest weather station, that station would always have available weather data.

This method would potentially skip over smaller weather stations that might be closer to a specific auto accident. However, because the major weather stations in NYC were within a 10-15 mile radius of all accidents in the NYC Motor Collisions dataset, this was an acceptable margin of error. I ultimately decided to focus on the core goals of the project: analyzing *general* trends. In addition, this method would avoid any potential data entry errors with the smaller weather stations. 

After filtering for completed weather data, the next step was to add weather data from the closest weather station to each accident in the NYC Collision dataset. I started by filtering the NYC Collisions dataset for accidents with non-Null and valid Latitude/Longitude coordinates (meaning coordinates that were actually *within* the city limits) which resulted in about 87.5%, or 1.75 million rows from the original dataset. I wrote the following Python script to accomplish this, which involved the use of the Haversine formula (which calculates the distance between two sets of Latitude and Longitude coordinates)

<details>
<summary>📜 Python Script – Nearest Weather Station Matching</summary>

```python
import pandas as pd
import logging
from math import radians, cos, sin, asin, sqrt

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

with open(r"NYC_CollisionsData", encoding='utf-8-sig') as Coll_Table:
    NYCdb = pd.read_csv(Coll_Table)
with open(r"NOAA Weather Data 95perc Accuracy.csv", encoding='utf-8-sig') as Weather_table:
    Stations = pd.read_csv(Weather_table)

NYCdb = NYCdb.rename(columns={'LATITUDE': 'NYCLat', 'LONGITUDE': 'NYCLong'})
Stations = Stations.rename(columns={'LATITUDE': 'StationLat', 'LONGITUDE': 'StationLong'})

NYCdb['CRASH_DATE'] = pd.to_datetime(NYCdb['CRASH_DATE']).dt.date
Stations['DATE'] = pd.to_datetime(Stations['DATE']).dt.date

NYCdb.sort_values(by='CRASH_DATE', inplace=True)
Stations.sort_values(by='DATE', inplace=True)

date_cache = {}

class Coordinates:
    def __init__(self, lat, lon):
        self.lat = radians(lat)
        self.lon = radians(lon)
    def __sub__(self, other):
        diff_lon = self.lon - other.lon
        diff_lat = self.lat - other.lat
        a = sin(diff_lat / 2) ** 2 + cos(other.lat) * cos(self.lat) * sin(diff_lon / 2) ** 2
        c = 2 * asin(sqrt(a))
        return 6371 * c  # Distance in km

def find_nearest_station_by_date(lat, lon, date):
    if date in date_cache:
        stations_on_date = date_cache[date]
    else:
        stations_on_date = Stations[Stations['DATE'] == date]
        date_cache.clear()
        date_cache[date] = stations_on_date
        if stations_on_date.empty or not date:
            return None
    src_coords = Coordinates(lat, lon)
    distances = stations_on_date.apply(
        lambda row: Coordinates(row['StationLat'], row['StationLong']) - src_coords, axis=1)
    closest_idx = distances.idxmin()
    return stations_on_date.loc[closest_idx, 'NAME']

NYCdb['Closest_Station'] = NYCdb.apply(
    lambda row: find_nearest_station_by_date(row['NYCLat'], row['NYCLong'], row['CRASH_DATE']), axis=1)

NYCdb.to_csv("output.csv", index=False, sep=',', encoding='utf-8')
```

</details>


This returned a table with the Collision ID of each auto accident within the city limits, the date of that accident, and the closest weather station to that accident. The rest of the weather data from the NOAA dataset could then be added to the main NYC Collisions dataset via a simple JOIN in SQL.

With this completed, approximately 87.5% of all auto accidents within the NYC Collisions dataset now had corresponding weather data. The remaining 12.5% of rows had missing or faulty coordinates, which I set out to fill in with averaged weather data based on the date of the accident.

In order to accomplish this, I used SQL to quickly process the dataset. This involved a lengthy script to reformat the needed rows (COALESCE), create a CTE with the average weather readings for the needed dates, and fianlly JOIN the CTE onto the main NYC Collisions dataset for the necessary rows. 

<details>
<summary>📜 SQL Script – Mean Inputation for Missing Weather Data</summary>

```sql
-- Update NULL data in WeatherData using COALESCE for later averaging
UPDATE [Over 90 Accurate Weather Data]
SET 
    [High_Winds]    = COALESCE([High_Winds], 0),
    [Precipitation] = COALESCE([Precipitation], 0),
    [Snowfall]      = COALESCE([Snowfall], 0),
    [Snow_Depth]    = COALESCE([Snow_Depth], 0),
    -- If Avg_Temp is missing, but Max_Temp and Min_Temp are present, calculate Avg from Max and Min
    [Avg_Temp]      = CASE 
                         WHEN [Avg_Temp] IS NULL AND [Max_Temp] IS NOT NULL AND [Min_Temp] IS NOT NULL 
                             THEN ([Max_Temp] + [Min_Temp]) / 2 
                         ELSE COALESCE([Avg_Temp], -99) 
                     END,
    [Min_Temp]      = CASE WHEN [Min_Temp] IS NULL THEN -99 ELSE [Min_Temp] END,
    [Max_Temp]      = CASE WHEN [Max_Temp] IS NULL THEN -99 ELSE [Max_Temp] END,
    [Fog]           = COALESCE([Fog], 0),
    [Heavy_Fog]     = COALESCE([Heavy_Fog], 0),
    [Thunder]       = COALESCE([Thunder], 0),
    [Sleet]         = COALESCE([Sleet], 0),
    [Hail]          = COALESCE([Hail], 0),
    [Glaze/Rime]    = COALESCE([Glaze/Rime], 0),
    [Smoke/Haze]    = COALESCE([Smoke/Haze], 0);

-- Fill remaining NULLs for Collisions table by applying safe defaults
-- (Temperature uses -99 because 0 is a legitimate Farenheit reading in NYC)
-- COALESCE is ommited in this case due to the number of specific conditions needed to properly update the Collisions table
UPDATE [Motor_Vehicle_Collisions_-_Crashes]
SET [Avg Temp] = -99
WHERE (
        [Avg Temp] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
        AND LATITUDE > 40
        AND LATITUDE < 41
        AND LONGITUDE > -74.5
        AND LONGITUDE < -73
    )
    OR (
        [Avg Temp] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
        AND LATITUDE IS NULL
    )
    OR (
        [Avg Temp] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
        AND LATITUDE = 0
    );

UPDATE [Motor_Vehicle_Collisions_-_Crashes]
SET [Min Temp] = -99
WHERE (
        [Min Temp] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
        AND LATITUDE > 40
        AND LATITUDE < 41
        AND LONGITUDE > -74.5
        AND LONGITUDE < -73
    )
    OR (
        [Min Temp] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
        AND LATITUDE IS NULL
    )
    OR (
        [Min Temp] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
        AND LATITUDE = 0
    );

UPDATE [Motor_Vehicle_Collisions_-_Crashes]
SET [Max Temp] = -99
WHERE (
        [Max Temp] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
        AND LATITUDE > 40
        AND LATITUDE < 41
        AND LONGITUDE > -74.5
        AND LONGITUDE < -73
    )
    OR (
        [Max Temp] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
        AND LATITUDE IS NULL
    )
    OR (
        [Max Temp] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
        AND LATITUDE = 0
    );

UPDATE [Motor_Vehicle_Collisions_-_Crashes]
SET [Fog] = 0
WHERE (
        [Fog] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
        AND LATITUDE > 40
        AND LATITUDE < 41
        AND LONGITUDE > -74.5
        AND LONGITUDE < -73
    )
    OR (
        [Fog] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
        AND LATITUDE IS NULL
    )
    OR (
        [Fog] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
        AND LATITUDE = 0
    );

UPDATE [Motor_Vehicle_Collisions_-_Crashes]
SET [Heavy Fog] = 0
WHERE (
        [Heavy Fog] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
        AND LATITUDE > 40
        AND LATITUDE < 41
        AND LONGITUDE > -74.5
        AND LONGITUDE < -73
    )
    OR (
        [Heavy Fog] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
        AND LATITUDE IS NULL
    )
    OR (
        [Heavy Fog] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
        AND LATITUDE = 0
    );

UPDATE [Motor_Vehicle_Collisions_-_Crashes]
SET [Precipitation] = 0
WHERE (
        [Precipitation] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
        AND LATITUDE > 40
        AND LATITUDE < 41
        AND LONGITUDE > -74.5
        AND LONGITUDE < -73
    )
    OR (
        [Precipitation] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
        AND LATITUDE IS NULL
    )
    OR (
        [Precipitation] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
        AND LATITUDE = 0
    );

UPDATE [Motor_Vehicle_Collisions_-_Crashes]
SET [Snowfall] = 0
WHERE (
        [Snowfall] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
        AND LATITUDE > 40
        AND LATITUDE < 41
        AND LONGITUDE > -74.5
        AND LONGITUDE < -73
    )
    OR (
        [Snowfall] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [Snowfall] > '2012-12-31 00:00:00.0000000'
        AND LATITUDE IS NULL
    )
    OR (
        [Snowfall] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [Snowfall] > '2012-12-31 00:00:00.0000000'
        AND LATITUDE = 0
    );

UPDATE [Motor_Vehicle_Collisions_-_Crashes]
SET [Snow Depth] = 0
WHERE (
        [Snow Depth] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
        AND LATITUDE > 40
        AND LATITUDE < 41
        AND LONGITUDE > -74.5
        AND LONGITUDE < -73
    )
    OR (
        [Snow Depth] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [Snow Depth] > '2012-12-31 00:00:00.0000000'
        AND LATITUDE IS NULL
    )
    OR (
        [Snow Depth] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [Snow Depth] > '2012-12-31 00:00:00.0000000'
        AND LATITUDE = 0
    );

UPDATE [Motor_Vehicle_Collisions_-_Crashes]
SET [Thunder] = 0
WHERE (
        [Thunder] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [Thunder] > '2012-12-31 00:00:00.0000000'
        AND LATITUDE > 40
        AND LATITUDE < 41
        AND LONGITUDE > -74.5
        AND LONGITUDE < -73
    )
    OR (
        [Thunder] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [Thunder] > '2012-12-31 00:00:00.0000000'
        AND LATITUDE IS NULL
    )
    OR (
        [Thunder] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [Thunder] > '2012-12-31 00:00:00.0000000'
        AND LATITUDE = 0
    );

UPDATE [Motor_Vehicle_Collisions_-_Crashes]
SET [Sleet] = 0
WHERE (
        [Sleet] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [Sleet] > '2012-12-31 00:00:00.0000000'
        AND LATITUDE > 40
        AND LATITUDE < 41
        AND LONGITUDE > -74.5
        AND LONGITUDE < -73
    )
    OR (
        [Sleet] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [Sleet] > '2012-12-31 00:00:00.0000000'
        AND LATITUDE IS NULL
    )
    OR (
        [Sleet] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [Sleet] > '2012-12-31 00:00:00.0000000'
        AND LATITUDE = 0
    );

UPDATE [Motor_Vehicle_Collisions_-_Crashes]
SET [Hail] = 0
WHERE (
        [Hail] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [Hail] > '2012-12-31 00:00:0000'
        AND LATITUDE > 40
        AND LATITUDE < 41
        AND LONGITUDE > -74.5
        AND LONGITUDE < -73
    )
    OR (
        [Hail] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:0000'
        AND [Hail] > '2012-12-31 00:00:0000'
        AND LATITUDE IS NULL
    )
    OR (
        [Hail] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:0000'
        AND [Hail] > '2012-12-31 00:00:0000'
        AND LATITUDE = 0
    );

UPDATE [Motor_Vehicle_Collisions_-_Crashes]
SET [Glaze/Rime] = 0
WHERE (
        [Glaze/Rime] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [Glaze/Rime] > '2012-12-31 00:00:00.0000000'
        AND LATITUDE > 40
        AND LATITUDE < 41
        AND LONGITUDE > -74.5
        AND LONGITUDE < -73
    )
    OR (
        [Glaze/Rime] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [Glaze/Rime] > '2012-12-31 00:00:0000'
        AND LATITUDE IS NULL
    )
    OR (
        [Glaze/Rime] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [Glaze/Rime] > '2012-12-31 00:00:0000'
        AND LATITUDE = 0
    );

UPDATE [Motor_Vehicle_Collisions_-_Crashes]
SET [Smoke/Haze] = 0
WHERE (
        [Smoke/Haze] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [Smoke/Haze] > '2012-12-31 00:00:0000'
        AND LATITUDE > 40
        AND LATITUDE < 41
        AND LONGITUDE > -74.5
        AND LONGITUDE < -73
    )
    OR (
        [Smoke/Haze] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:0000'
        AND [Smoke/Haze] > '2012-12-31 00:00:0000'
        AND LATITUDE IS NULL
    )
    OR (
        [Smoke/Haze] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:0000'
        AND [Smoke/Haze] > '2012-12-31 00:00:0000'
        AND LATITUDE = 0
    );

UPDATE [Motor_Vehicle_Collisions_-_Crashes]
SET [HighWinds] = 0
WHERE (
        [HighWinds] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [HighWinds] > '2012-12-31 00:00:0000'
        AND LATITUDE > 40
        AND LATITUDE < 41
        AND LONGITUDE > -74.5
        AND LONGITUDE < -73
    )
    OR (
        [HighWinds] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [HighWinds] > '2012-12-31 00:00:0000'
        AND LATITUDE IS NULL
    )
    OR (
        [HighWinds] IS NULL
        AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
        AND [HighWinds] > '2012-12-31 00:00:0000'
        AND LATITUDE = 0
    );

-- Create a CTE with average weather data grouped by date (this will be used to fill in Collisions table with missing locations)
WITH AVG_WeatherData_By_Date AS (
    SELECT 
        [DATE] AS [Temp Date],
        AVG([Precipitation]) AS [Temp Precipitation],
        AVG([Snowfall]) AS [Temp Snowfall],
        AVG(Snow_Depth) AS [Temp Snow Depth],
        AVG(Avg_Temp) AS [Temp Avg Temp],
        AVG([Max_Temp]) AS [Temp Max Temp],
        AVG([Min_Temp]) AS [Temp Min Temp],
        AVG([Heavy_Fog]) AS [Temp Heavy Fog],
        AVG([High_Winds]) AS [Temp High Winds],
        AVG([Fog]) AS [Temp Fog],
        AVG([Thunder]) AS [Temp Thunder],
        AVG([Sleet]) AS [Temp Sleet],
        AVG([Hail]) AS [Temp Hail],
        AVG([Glaze/Rime]) AS [Temp Glaze/Rime],
        AVG([Smoke/Haze]) AS [Temp Smoke/Haze]
    FROM [Over 90 Accurate Weather Data]
    WHERE [DATE] IN (
        SELECT [DATE]
        FROM [Motor_Vehicle_Collisions_-_Crashes] AS TargetTable
        WHERE 
            TargetTable.CRASH_DATE < '2024-01-01 00:00:00.0000000'
            AND TargetTable.CRASH_DATE > '2012-12-31 00:00:00.0000000'
            AND (
                TargetTable.LATITUDE > 40
                AND TargetTable.LATITUDE < 41
                AND TargetTable.LONGITUDE > -74.5
                AND TargetTable.LONGITUDE < -73
                AND TargetTable.Closest_Station IS NULL
            )
            OR TargetTable.LATITUDE IS NULL
    )
    GROUP BY [DATE]
)

-- Add the average weather data to Collisions table for rows that still have missing station matches
UPDATE TargetTable
SET
    TargetTable.Precipitation = WeatherData.[Temp Precipitation],
    TargetTable.Snowfall = WeatherData.[Temp Snowfall],
    TargetTable.[Snow Depth] = WeatherData.[Temp Snow Depth],
    TargetTable.[Avg Temp] = WeatherData.[Temp Avg Temp],
    TargetTable.[Max Temp] = WeatherData.[Temp Max Temp],
    TargetTable.[Min Temp] = WeatherData.[Temp Min Temp],
    TargetTable.HighWinds = WeatherData.[Temp High Winds],
    TargetTable.Fog = WeatherData.[Temp Fog],
    TargetTable.[Heavy Fog] = WeatherData.[Temp Heavy Fog],
    TargetTable.Thunder = WeatherData.[Temp Thunder],
    TargetTable.Sleet = WeatherData.[Temp Sleet],
    TargetTable.Hail = WeatherData.[Temp Hail],
    TargetTable.[Glaze/Rime] = WeatherData.[Temp Glaze/Rime],
    TargetTable.[Smoke/Haze] = WeatherData.[Temp Smoke/Haze]
FROM [Motor_Vehicle_Collisions_-_Crashes] AS TargetTable
JOIN AVG_WeatherData_By_Date AS WeatherData
    ON TargetTable.CRASH_DATE = WeatherData.[Temp Date]
WHERE
    TargetTable.CRASH_DATE < '2024-01-01 00:00:00.0000000'
    AND TargetTable.CRASH_DATE > '2012-12-31 00:00:00.0000000'
    AND (
        TargetTable.LATITUDE > 40
        AND TargetTable.LATITUDE < 41
        AND TargetTable.LONGITUDE > -74.5
        AND TargetTable.LONGITUDE < -73
        AND TargetTable.Closest_Station IS NULL
    )
    OR TargetTable.LATITUDE IS NULL;
```

</details>
