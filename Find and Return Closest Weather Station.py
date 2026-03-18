import pandas as pd
import logging
from math import radians, cos, sin, asin, sqrt

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#logger.info('Starting script')

# Load data
#logger.debug('Loading and processing data')
with open(r"C:\Users\menes\Documents\NYC Traffic Collisions Project\Weather Data\DB_Coll_ID_w_Coordinates.csv", encoding='utf-8-sig') as Coll_Table:
    NYCdb = pd.read_csv(Coll_Table)
with open(r"C:\Users\menes\Documents\NYC Traffic Collisions Project\Weather Data\Over 90 Accurate Weather Data.csv", encoding='utf-8-sig') as Weather_table:
    Stations = pd.read_csv(Weather_table)

# Rename columns
NYCdb = NYCdb.rename(columns={'LATITUDE': 'NYCLat', 'LONGITUDE': 'NYCLong'})
Stations = Stations.rename(columns={'LATITUDE': 'StationLat', 'LONGITUDE': 'StationLong'})
#logger.debug('Columns renamed')

# Fix date formats
NYCdb['CRASH_DATE'] = pd.to_datetime(NYCdb['CRASH_DATE']).dt.date
Stations['DATE'] = pd.to_datetime(Stations['DATE']).dt.date
#logger.debug('Dates reformatted')

#Sort data by Date, allows caching by groups of same dates
NYCdb.sort_values(by='CRASH_DATE', inplace=True)
Stations.sort_values(by='DATE', inplace=True)
#logger.debug('Data sorted by date')

#create the date cache
date_cache = {}

# function to apply Haversine formula to find the distance between two sets of latitude/longitude coordinates:
class Coordinates:
    def __init__(self, lat, lon):
        self.lat = radians(lat)
        self.lon = radians(lon)

    def __repr__(self):
        return f"Coordinates(lat={self.lat}, lon={self.lon})"

    # Haversine formula
    def __sub__(self, other):
        diff_lon = self.lon - other.lon
        diff_lat = self.lat - other.lat
        a = sin(diff_lat / 2) ** 2 + cos(other.lat) * cos(self.lat) * sin(diff_lon / 2) ** 2
        c = 2 * asin(sqrt(a))
        return 6371 * c

# Main function, returns the closest weather station that has a weather reading
# for the date in the current row in NYCdb
def find_nearest_station_by_date(lat, lon, date):
    #logger.info(f'Finding nearest station for lat: {lat}, lon: {lon}, date: {date}')

    # logical checks are in the order of most likely -> least likely to occur:
    # First checks if Date is already in cache, which happens most often
    # Second, checks if the date is a date, but a different one (when the code encounters a new date in the sorted table)
    # Then if that new date is empty, due to the row not having a date or no stations were found for the date, return None
    if date in date_cache:
        stations_on_date = date_cache[date]

    else:
        stations_on_date = Stations[Stations['DATE'] == date]
        date_cache.clear()  # only cache one date at a time to conserve memory
        date_cache[date] = stations_on_date

        if stations_on_date.empty:
            #logger.warning(f'No stations found for date: {date}')
            return None

        elif not date:
            #logger.warning('Date is None, skipping row')
            return None

    # Find the distance between the current accident (row) and all stations that have weather readings for the current accident date
    src_coords = Coordinates(lat, lon)
    distances = stations_on_date.apply(
        lambda row: Coordinates(row['StationLat'], row['StationLong']) - src_coords, axis=1)

    # Then, return the station with the shortest distance
    closest_idx = distances.idxmin()
    station_name = stations_on_date.loc[closest_idx, 'NAME']
    #logger.info(f'Found nearest station: {station_name} for date: {date}')
    return stations_on_date.loc[closest_idx, 'NAME']

# Applies the main function to the NYCdb table, and adds the returned Station name
# to the current row in a new column 'Closest_Station'
NYCdb['Closest_Station'] = NYCdb.apply(
    lambda row: find_nearest_station_by_date(row['NYCLat'], row['NYCLong'], row['CRASH_DATE']), axis=1)

# Save to CSV
logger.info('Saving to CSV')
NYCdb.to_csv("output.csv", index=False, sep=',', encoding='utf-8')
logger.info('done')
