from enum import Enum
import requests, yaml, time, os
import numpy as np
import pandas as pd
from google.transit import gtfs_realtime_pb2
from datetime import datetime
import duckdb
import fsspec


# Original function by Deduplicator: stackoverflow.com/questions/27928/calculate-distance-between-two-latitude-longitude-points-haversine-formula/21623206
# Adapted to calculate speed between two points

def speed(data):
    R = 6371 # Radius of the earth in km
    
    data['lat2'] = data['latitude'].shift(-1)
    data['lon2'] = data['longitude'].shift(-1)
    data['time2'] = data['timestamp'].shift(-1) 

    lat1 = data['latitude']
    lon1 = data['longitude']
    lat2 = data['lat2']
    lon2 = data['lon2']
    
    data['haversine_lat'] = np.subtract(data['lat2'], data['latitude'])
    data['haversine_lon'] = np.subtract(data['lon2'], data['longitude'])
    
    dLat = deg2rad(data['haversine_lat'])  
    dLon = deg2rad(data['haversine_lon'])

    a = np.sin(dLat/2) * np.sin(dLat/2) + np.cos(deg2rad(lat1)) * np.cos(deg2rad(lat2)) * np.sin(dLon/2) * np.sin(dLon/2)
    
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a)) 
    d = R * c * 1000

    time_delta = (data['time2'] - data['timestamp']).dt.total_seconds()
    speed = d/time_delta
    
    return speed

#Adapted from Deduplicator: stackoverflow.com/questions/27928/calculate-distance-between-two-latitude-longitude-points-haversine-formula/
def deg2rad(deg):
    return deg * (np.pi/180)


def main():
    database = duckdb.connect('/home/anthony/Documents/CMPT 353/Project/transit.db')

    routes_df = database.sql('SELECT * FROM routes').df()
    routes_df = routes_df.astype('string')
    routes_df['transit_type'] = routes_df['route_type'].map({'2': 'West Coast Express', '3': 'Bus', '4': 'SeaBus'})
    routes_df.loc[routes_df.route_long_name.str.contains('Canada Line'), 'transit_type'] = 'SkyTrain (Canada Line)'
    routes_df.loc[routes_df.route_long_name.str.contains('Millennium Line'), 'transit_type'] = 'SkyTrain (Millenium Line)'
    routes_df.loc[routes_df.route_long_name.str.contains('Expo Line'), 'transit_type'] = 'SkyTrain (Expo Line)'
    routes_df.loc[routes_df.route_long_name.str.contains('B-Line'), 'transit_type']  = 'Bus (B-Line)'
    routes_df.loc[routes_df.route_short_name.str.contains('R'), 'transit_type'] = 'Bus (RapidBus)'

    stops_df = database.sql('SELECT * FROM stops').df()
    stops_df['wheelchair_accessible'] = stops_df.wheelchair_boarding.astype('int64').map({1: 'Accessible', 2: 'Non-accessible'})
    popular_stops = database.sql('SELECT stop_id, count(*) FROM rt_position GROUP BY stop_id ORDER BY count(*)').df()
    popular_stops = popular_stops.rename(columns={'count_star()': 'times_visited'})

    # Merging popular stop statistics with the rest of stop data
    stop_frequency = pd.merge(popular_stops.astype('string'), stops_df.astype('string'), on='stop_id', how='left')
    stop_frequency['times_visited'] = stop_frequency['times_visited'].astype('int64')
    stop_frequency.to_csv('stop_frequency.csv')
    
    
    position_df = database.sql('SELECT * FROM rt_position').df()

    #Removing data with invalid coordinates
    position_df = position_df[(position_df['latitude'] >= 49) &  (position_df['latitude'] <= 49.5) & (position_df['longitude'] >= -123.5) & (position_df['longitude'] <= -122.3)]

    trip_speeds = pd.DataFrame(position_df.groupby(['trip_id', 'route_id']).apply(speed)).reset_index()
    trip_speeds = trip_speeds.dropna()
    trip_averages = pd.DataFrame(trip_speeds.groupby(['trip_id', 'route_id'])[0].mean()).reset_index()
    trip_averages = trip_averages.rename(columns={0: 'm/s'})
    
    #Removing trips that either didn't leave the station or didn't have their coordinates updated
    trip_averages = trip_averages[trip_averages['m/s'] >= 1]
    trip_averages.sort_values('m/s')

    route_averages = pd.DataFrame(trip_averages.groupby('route_id')['m/s'].mean()).reset_index()
    route_averages['km/h'] = (route_averages['m/s']*3.6)
    routes_speeds = pd.merge(route_averages.astype('string'), routes_df.astype('string'), on='route_id', how='left')
    routes_speeds['km/h'] = routes_speeds['km/h'].astype('float64')
    routes_speeds = routes_speeds.sort_values('km/h', ascending=False)
    bus_average = np.mean(routes_speeds['km/h']) 
    routes_speeds.to_csv('routes_speeds.csv')
    
    boardings = pd.read_csv('2024-boardings-by-servic.csv')
    boardings['SkyTrain'] = (boardings['Canada Line'] + boardings['Expo and Millennium Line'])
    boardings = boardings.melt(id_vars=['Category'], var_name='transit_type', value_name='ridership')
    boardings = boardings[~boardings['transit_type'].isin(['Canada Line', 'Expo and Millennium Line'])]
    boardings = pd.DataFrame(boardings.groupby('transit_type')['ridership'].mean()).reset_index()    
    boardings['transit_type'] = boardings['transit_type'].astype('string')

    trips_routes = database.sql('SELECT trips.*, route_type FROM trips LEFT JOIN routes USING (route_id)')
    trips_share = database.sql('SELECT route_type, count(*) FROM trips_routes GROUP BY route_type ORDER BY count(*)').df()
    trips_share = trips_share[trips_share.route_type != '715']
    trips_share['transit_type'] = trips_share.route_type.astype('int64').map({1: 'SkyTrain', 2: 'WCE', 3: 'Bus', 4: 'SeaBus'})
    
    trips_ridership = pd.merge(trips_share, boardings, on='transit_type')
    trips_ridership = trips_ridership.rename(columns={'count_star()': 'trips'})
    trips_ridership['weekly_ridership'] = (trips_ridership['ridership'] / 4.33)
    trips_ridership['riders/trip'] = np.round((trips_ridership['weekly_ridership']/trips_ridership['trips'])*1000000)
    trips_ridership['trips/rider'] = np.round((trips_ridership['trips']/trips_ridership['weekly_ridership'])/10)
    trips_ridership.to_csv('trips_ridership.csv')
    
    
    
if __name__=='__main__':
    main()

    
