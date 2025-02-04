import sys, duckdb, os, tempfile,kaleido
from zipfile import ZipFile, BadZipFile
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime

def clean(db:duckdb)-> None:
    clean_queries = {
        'rt_trip_1970': """DELETE FROM rt_trip WHERE year(next_stop_arrival_time) == 1970""",
        'rt_position_1970': """DELETE FROM rt_position WHERE year(timestamp) = 1970""",
        'rt_position_filter_max_latitude': """DELETE FROM rt_position WHERE latitude < 49 """,
        'rt_position_filter_min_latitude': """DELETE FROM rt_position WHERE latitude > 49.5 """,
        'rt_position_filter_max_longitude': """DELETE FROM rt_position WHERE longitude < -123.5 """,
        'rt_position_filter_min_longitude': """DELETE FROM rt_position WHERE longitude > -122.3 """,
        'shapes_filter_not_handydart': """DELETE FROM shapes WHERE shape_id = '4484' """
    }

    for query in clean_queries.values():
        db.execute(query)

def analyze(db:duckdb,call:str, args:str) -> None:
    if call == 'map_average_speeds':
        average_speeds_query = """SELECT        
                                        trips.trip_id,
                                        stops.stop_id,
                                        stop_sequence,
                                        arrival_time::interval as arrival_time,
                                        ifnull(route_short_name,route_long_name) as route_name,

                                        COALESCE(
                                                ifnull(shape_dist_traveled, 0) - LAG(ifnull(shape_dist_traveled, 0)) OVER (
                                                        PARTITION BY trips.trip_id
                                                        ORDER BY stop_sequence::int
                                        )) / (epoch(COALESCE(
                                                arrival_time::interval - LAG(arrival_time::interval) OVER (
                                                        PARTITION BY trips.trip_id
                                                        ORDER BY stop_sequence::int
                                                )))/3600) as avg_speed,
                                        COALESCE(
                                                stop_lat + LAG(stop_lat) OVER (
                                                        PARTITION BY route_long_name
                                                        ORDER BY stop_sequence::int
                                                ))/2 as midpoint_latitude,
                                        COALESCE(
                                                stop_lon + LAG(stop_lon) OVER (
                                                        PARTITION BY route_long_name
                                                        ORDER BY stop_sequence::int
                                                ))/2 as midpoint_longitude
                                FROM stop_times
                                INNER JOIN trips
                                ON trips.trip_id = stop_times.trip_id
                                INNER JOIN routes
                                ON routes.route_id = trips.route_id
                                INNER JOIN stops
                                ON stops.stop_id = stop_times.stop_id
                                ORDER BY avg_speed Desc"""
        
        average_speeds = db.execute(average_speeds_query).df().dropna()
        average_speeds = average_speeds[average_speeds['arrival_time'].dt.components.hours == args]
        fig = go.Figure()
        fig = px.scatter_map(average_speeds,
                                lat="midpoint_latitude",
                                lon="midpoint_longitude", 
                                color="avg_speed",
                                center=dict(lat=49.25, lon=-123), 
                                zoom=10)

        fig.update_layout(map_style="open-street-map",
                        
            margin={"r":0,"t":0,"l":0,"b":0})
        fig.write_image("images/average_speeds_points.jpg",scale=10, width=1440, height=1080)

            

if __name__=='__main__':
    db_name = sys.argv[1]
    call = sys.argv[2]
    args = sys.argv[3]
    db = duckdb.connect(db_name)
    clean(db)
    analyze(db,call,args)
    db.close()