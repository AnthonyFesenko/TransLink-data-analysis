import pandas as pd
import duckdb
from gtfs_functions import Feed


# Prepares data to be used for building the route map
def main():
    database = duckdb.connect('transit.db')

    feed = Feed("transit_cleaned.zip")
    feed_stops = feed.stops
    feed_shapes = feed.shapes
    feed_shapes['shape_id'] = feed_shapes['shape_id'].astype('string')

    trips = database.sql('SELECT * FROM trips').df().astype('string')
    routes = database.sql('SELECT * FROM routes').df().astype('string')
    
    routes = routes[routes['route_id'] != 'HD']
    trips = trips[trips['route_id'] != 'HD']

    routes['transit_type'] = routes['route_type'].map({'2': 'West Coast Express', '3': 'Bus', '4': 'SeaBus'})
    routes.loc[routes['route_long_name'].str.contains('Canada Line'), 'transit_type'] = 'SkyTrain (Canada Line)'
    routes.loc[routes['route_long_name'].str.contains('Millennium Line'), 'transit_type'] = 'SkyTrain (Millenium Line)'
    routes.loc[routes['route_long_name'].str.contains('Expo Line'), 'transit_type'] = 'SkyTrain (Expo Line)'
    routes.loc[routes['route_long_name'].str.contains('B-Line'), 'transit_type']  = 'Bus (B-Line)'
    routes.loc[routes['route_short_name'].str.contains('R'), 'transit_type'] = 'Bus (RapidBus)'

    trips_routes= pd.merge(trips[['route_id', 'shape_id']], routes[['route_id', 'route_short_name', 'route_long_name', 'transit_type', ]], on='route_id', how='left')
    shape_info = pd.merge(feed_shapes, trips_routes, on='shape_id', how='left')
    shape_info = shape_info.drop_duplicates()
    shape_info.to_csv('shape_info.csv')
    feed_stops.to_csv('feed_stops.csv')
    
if __name__ == '__main__':
    main()



