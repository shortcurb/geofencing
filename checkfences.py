
import  csv,os,shutil,json
import math
import folium
import geopandas as gpd
from support_connections.database import Database
#from support_connections.apis import Hasura
from support_connections.record import RecordFetcher
from dotenv import load_dotenv
from haversine import inverse_haversine,Unit,Direction
from shapely import wkt
from shapely.geometry import Point, Polygon, LineString, MultiLineString,MultiPolygon
from shapely.ops import unary_union
from geopy.distance import distance
from shapely.wkt import dumps,loads
import pandas as pd


import datetime,json,pytz,os,requests,hashlib,time
from support_connections.database import Database
#from support_connections.apis import Hasura
from support_connections.record import RecordFetcher
from dotenv import load_dotenv
from haversine import haversine,Unit

load_dotenv()
db = Database()
rec = RecordFetcher()
coordinates = []


def readshp(filename):
    gpddata = gpd.read_file(filename)
    gpddata = gpddata.to_crs(epsg=32633)
#    gpddata = gpddata.to_crs(epsg=4326)
    return(gpddata)

buffers = {
    'buffer_tx_mexico_north':0,
    'buffer_tx_mexico_south':0,
    'buffer_nmaz_mexico_north':0,
    'buffer_nmaz_mexico_south':0,
    'buffer_ca_mexico_north':0,
    'buffer_ca_mexico_south':0,
}


for name in buffers.keys():
    fn = f"polygons/{name}/{name}.shp"
    bufferzone = readshp(fn)
    buffers.update({name:bufferzone.to_crs(epsg=4326)})

def checkbuffers(coords):
    point = Point(coords[::-1])
    for name,buffer in buffers.items():
        iswithin = buffer.contains(point).any()
#        print(name,iswithin)


def checkexisting():
    checkablevehicles = db.execute_query('SELECT v.fleetnumber,v.vin,v.ismi,f.currentlyviolating FROM vehicles v LEFT JOIN fencedvehicles f ON v.vin=f.vin WHERE v.ismi != "" AND v.decomstatus NOT LIKE "DEC6%" AND v.decomstatus NOT LIKE "DEC7%" AND (v.market = "San Antonio" OR v.market = "Austin" OR v.market = "Houston") AND (f.currentlyviolating = False OR f.currentlyviolating is Null) LIMIT 5')
    for veh in checkablevehicles:
        a = datetime.datetime.now()
        latest_record = rec.get_and_parse_latest_record(veh['ismi'])
        b = datetime.datetime.now()
        lrcoords = (latest_record.event_data_GpsItems_Latitude,latest_record.event_data_GpsItems_Longitude)
        checkbuffers(lrcoords)
        c = datetime.datetime.now()
        print('record retrieval',(b-a).total_seconds(),'iswithin checks',(c-b).total_seconds())



checkexisting()