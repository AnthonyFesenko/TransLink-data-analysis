import requests, yaml, time
import numpy as np
from google.transit import gtfs_realtime_pb2
from datetime import datetime


def duplicate_check(response,gtfs_feed):
    now = datetime.now().strftime("%m/%d, %H:%M:%S")
    arr_test = np.zeros(100000000)
    status_failure_flag = 0 # if response shows 4 failures in a row, exit

    if response.status_code == 200:
        gtfs_feed.ParseFromString(response.content)
        for item in gtfs_feed.entity:
            index = int(item.id)
            if arr_test[index] == 0:
                arr_test[index] += 1
            else:
                raise SystemExit('More than one entity with trip id ' + str(index) + ' was found.')
        status_failure_flag = 0
        print('no duplicates at ' + now + ' : Feed length is: ' + str(len(gtfs_feed.entity)))
        
    else:
        if status_failure_flag < 4:
            raise Warning('failure in response at ' + now)
        else:
            raise SystemExit('4 consecutive failures in response at ' + now)

def main():
    Provider = 'Translink' # use to change data provider

    #create full url from auth.yaml
    with open('.config/my_api_keys/auth.yaml', 'r') as file:
        auth = yaml.load(file,Loader=yaml.SafeLoader)

    url = auth[Provider]['alerts_link'] + auth[Provider]['api_key0']

    #create gtfs feed
    feed = gtfs_realtime_pb2.FeedMessage()

    while True:
        response = requests.get(url)
        # Check if there are duplicates in any trip_id
        duplicate_check(response,feed)
        time.sleep(150)

if __name__=='__main__':
    main()