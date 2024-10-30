import requests
import yaml
from google.transit import gtfs_realtime_pb2

Provider = 'Translink' # use to change data provider

#create full url from auth.yaml
with open('.config/my_api_keys/auth.yaml', 'r') as file:
    auth = yaml.load(file,Loader=yaml.SafeLoader)     
url = auth[Provider]['link'] + auth[Provider]['api_key']

#create gtfs feed
feed = gtfs_realtime_pb2.FeedMessage()

#create full link to api including api key 

#print(url)


response = requests.get(url)
if response.status_code == 200:
    #data = response.json() #change to protobuf

    print(response.content)


#print(response)
