import sys, duckdb, os, tempfile
from zipfile import ZipFile, BadZipFile

queries = {
        'agency': "CREATE TABLE agency AS SELECT * FROM read_csv(?)",

        'calendar_dates' : """CREATE TABLE calendar_dates AS SELECT * FROM 
                read_csv(?,
                        header = true,
                        dateformat = '%Y%m%d',
                        columns = {
                                'service_id': 'VARCHAR',
                                'date': 'DATE',
                                'exception_type': 'VARCHAR'
                        }
                )""",

        'calendar': """CREATE TABLE calendar AS SELECT * FROM 
                read_csv(?,
                        header = true,
                        dateformat = '%Y%m%d',
                        columns = {
                                        'service_id': 'VARCHAR',
                                        'monday': 'BOOLEAN',
                                        'tuesday': 'BOOLEAN',
                                        'wednesday': 'BOOLEAN',
                                        'thursday': 'BOOLEAN',
                                        'friday': 'BOOLEAN',
                                        'saturday': 'BOOLEAN',
                                        'sunday': 'BOOLEAN',
                                        'start_date': 'DATE',
                                        'end_date': 'DATE'
                                })""",

        'direction_names_exceptions': """CREATE TABLE direction_names_exceptions AS SELECT * FROM 
                read_csv(?,
                        dateformat = '%Y%m%d',
                        all_varchar = True)""",

        'directions': """CREATE TABLE directions AS SELECT * FROM 
                read_csv(?,
                        dateformat = '%Y%m%d',
                        all_varchar = True)""",

        'feed_info': "CREATE TABLE feed_info AS SELECT * FROM read_csv(?,dateformat = '%Y%m%d')",

        'route_names_exceptions': """CREATE TABLE route_names_exceptions AS SELECT * FROM 
                read_csv(?,
                        dateformat = '%Y%m%d',
                        all_varchar = True)""",

        'routes': """CREATE TABLE routes AS SELECT * FROM 
                read_csv(?,
                        dateformat = '%Y%m%d',
                        all_varchar = True)""",

        'shapes': """CREATE TABLE shapes AS SELECT * FROM read_csv(?,
                        header = true,
                        dateformat = '%Y%m%d',
                        columns = {
                                        'shape_id': 'VARCHAR',
                                        'shape_pt_lat': 'DOUBLE',
                                        'shape_pt_lon': 'DOUBLE',
                                        'shape_pt_sequence': 'INTEGER',
                                        'shape_dist_traveled': 'DOUBLE'
                                })""",

        'signup_periods': """CREATE TABLE signup_periods AS SELECT * FROM read_csv(?,
                        header = true,
                        dateformat = '%Y%m%d',
                        columns = {
                                        'sign_id': 'VARCHAR',
                                        'from_date': 'DATE',
                                        'to_date': 'DATE'
                                })""",

        'stop_order_exceptions': """CREATE TABLE stop_order_exceptions AS SELECT * FROM 
                        read_csv(?, all_varchar = True)""",

        'stop_times': """CREATE TABLE stop_times AS SELECT * FROM read_csv(?,
                    header = true,
                    dateformat = '%Y%m%d',
                    columns = {
                                'trip_id': 'VARCHAR',
                                'arrival_time': 'VARCHAR',
                                'departure_time': 'VARCHAR',
                                'stop_id': 'VARCHAR',
                                'stop_sequence': 'VARCHAR',
                                'stop_headsign': 'VARCHAR',
                                'pickup_type': 'VARCHAR',
                                'drop_off_type': 'VARCHAR',
                                'shape_dist_traveled': 'DOUBLE',
                                'timepoint': 'VARCHAR'
                            })""",
        
        'stops': """CREATE TABLE stops AS SELECT * FROM read_csv(?,
                    header = true,
                    dateformat = '%Y%m%d',
                    columns = {
                                'stop_id': 'VARCHAR',
                                'stop_code': 'VARCHAR',
                                'stop_name': 'VARCHAR',
                                'stop_desc': 'VARCHAR',
                                'stop_lat': 'DOUBLE',
                                'stop_lon': 'DOUBLE',
                                'zone_id': 'VARCHAR',
                                'stop_url': 'VARCHAR',
                                'location_type': 'DOUBLE',
                                'parent_station': 'VARCHAR',
                                'wheelchair_boarding': 'VARCHAR'
                            })""",
        'transfers':"""CREATE TABLE transfers AS SELECT 
                                                from_stop_id,
                                                to_stop_id,
                                                transfer_type,
                                                (min_transfer_time || ' seconds')::INTERVAL as min_transfer_time,
                                                from_trip_id,
                                                to_trip_id
                                       
                                        FROM read_csv(?,
                                                header = true,
                                                dateformat = '%Y%m%d',
                                                columns = {
                                                                'from_stop_id': 'VARCHAR',
                                                                'to_stop_id': 'VARCHAR',
                                                                'transfer_type': 'VARCHAR',
                                                                'min_transfer_time': 'VARCHAR',
                                                                'from_trip_id': 'VARCHAR',
                                                                'to_trip_id': 'VARCHAR'
                                                        })""",
        'trips': "CREATE TABLE trips AS SELECT * FROM read_csv(?, all_varchar = True)"
}

def drop_existing(db:duckdb) -> None:

    for name in queries.keys():
        dropquery = f"DROP TABLE IF EXISTS {name}"
        db.execute(dropquery)

def extractor(gtfs_zip:str) -> str:
    extract_to = tempfile.mkdtemp()

    try:
        with ZipFile(gtfs_zip,'r') as file:
            file.extractall(extract_to)
    except BadZipFile:
        raise Exception("Recieved a zip file of bad type")
    
    return extract_to

def add_new(filepath:str,db:duckdb) -> None:
    # only allows one translink static file to be present in the database,
    # This is intended behavior for the current version but will be expanded as a personal project
    for name,query in queries.items():
        full_path = filepath + "/" + name + ".txt"
        db.execute(query, [full_path])

def run(gtfs_zip:str, db:duckdb) -> None:
    
    filepath = extractor(gtfs_zip)
    drop_existing(db)
    add_new(filepath,db)


if __name__=='__main__':
    gtfs_zip = sys.argv[1]
    db_name = sys.argv[2]
    db = duckdb.connect(db_name)
    run(gtfs_zip,db)
    db.close()