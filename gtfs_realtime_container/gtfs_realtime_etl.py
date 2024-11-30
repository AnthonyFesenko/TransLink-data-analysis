from etl_helper import *
import requests, yaml, duckdb, sched, time
from google.transit import gtfs_realtime_pb2


    
scheduler = sched.scheduler(time.time, time.sleep)


def schedule_pos_ingest(n,file,db):                                      #This function was roughly based on https://stackoverflow.com/questions/50264230/how-to-schedule-different-tasks-at-different-times-in-never-ending-program
    """
    Ingest positional data every n minutes
    
    Args:
        n (int): Number of minutes between executions
        file (file obj): yaml file containing api keys
        db (duckdb db): transit database object to insert data 
    """
    url = Rotated_api_link('Translink',file,'position_link')
    feed = get_feed(url)
    insert_rt_position(db, feed)
    # Schedule next execution in n minutes (n * 60 seconds)
    scheduler.enter(n * 60, 0, schedule_pos_ingest, (n,file,db))

def schedule_trip_ingest(n,file,db):                                      #This function was roughly based on https://stackoverflow.com/questions/50264230/how-to-schedule-different-tasks-at-different-times-in-never-ending-program
    """
    Ingest trip update data every n minutes
    
    Args:
        n (int): Number of minutes between executions
        file (file obj): yaml file containing api keys
        db (duckdb db): transit database object to insert data 
    """
    url = Rotated_api_link('Translink',file,'trip_link')
    feed = get_feed(url)
    insert_rt_trip(db, feed)
    # Schedule next execution in n minutes (n * 60 seconds)
    scheduler.enter(n * 60, 0, schedule_trip_ingest, (n,file,db))

def schedule_alert_ingest(n,file,db):                                      #This function was roughly based on https://stackoverflow.com/questions/50264230/how-to-schedule-different-tasks-at-different-times-in-never-ending-program
    """
    Ingest service alert data every n minutes
    
    Args:
        n (int): Number of minutes between executions
        file (file obj): yaml file containing api keys
        db (duckdb db): transit database object to insert data 
    """
    url = Rotated_api_link('Translink',file,'alerts_link')
    feed = get_feed(url)
    insert_rt_alerts(db, feed)
    # Schedule next execution in n minutes (n * 60 seconds)
    scheduler.enter(n * 60, 0, schedule_alert_ingest, (n,file,db))


def main():
    #create full url from auth.yaml
    with open('auth.yaml', 'r') as file:    #TODO put link in gtfs_realtime_etl.config
        auth = yaml.load(file,Loader=yaml.SafeLoader)

    dir = 'transit.db'
    transit_db = create_db(dir)
    
    scheduler.enter(0, 0, schedule_pos_ingest, (1,auth,transit_db))
    scheduler.enter(30, 0, schedule_trip_ingest, (1,auth,transit_db))
    scheduler.enter(15, 0, schedule_alert_ingest, (30,auth,transit_db))

    scheduler.run()
    
if __name__=='__main__':
    main()