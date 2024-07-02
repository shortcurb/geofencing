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


load_dotenv()
db = Database()
rec = RecordFetcher()
coordinates = []

'''
Best way to create a border is to download one of the land borders here
https://www.naturalearthdata.com/downloads/10m-cultural-vectors/
Then upload the files to here 
https://mapshaper.org/
Delete whatever stuff you don't want, save it
Then upload the .shp and .shx files here, point the code to the .shp file

'''

def cali_south_fixer(buffer_ca_mexico_south):
    point1 = [32.538433, -117.081516]
    point2 = [33.117610, -118.122772]

    min_lon = min(point1[1], point2[1])
    max_lon = max(point1[1], point2[1])
    min_lat = min(point1[0], point2[0])
    max_lat = max(point1[0], point2[0])
    corners = [(min_lon, min_lat), (min_lon, max_lat), (max_lon, max_lat), (max_lon, min_lat), (min_lon, min_lat)]
    polygon = Polygon(corners)
    sandiego = gpd.GeoDataFrame(index=[0],crs="EPSG:4326", geometry=[polygon])
    sandiego.to_crs(epsg=32633, inplace=True)
    buffer_ca_mexico_south = gpd.overlay(buffer_ca_mexico_south,sandiego,how = 'difference')
    return(buffer_ca_mexico_south)

def az_north_fixer(buffer_nmaz_mexico_north):
    points = [
        [32.721177, -114.727665],
        [32.721652, -114.554344],
        [32.739470, -114.530699],
        [32.791662, -114.531685],
        [32.815676, -114.511242],
        [32.844650, -114.468633],
        [32.910428, -114.463214],
        [33.027243, -114.518485],
        [32.721177, -114.727665],
    ]
    cpoints = []
    for point in points:
        cpoints.append(point[::-1])
    polygon = Polygon(cpoints)
    imperal_sand_dunes = gpd.GeoDataFrame(index=[0],crs="EPSG:4326", geometry=[polygon])
    imperal_sand_dunes.to_crs(epsg=32633, inplace=True)
    buffer_nmaz_mexico_north = gpd.overlay(buffer_nmaz_mexico_north,imperal_sand_dunes,how = 'difference')
    return(buffer_nmaz_mexico_north)

def tx_north_fixer(buffer_tx_mexico_north):
    points = [
        [31.783934, -106.529105],
        [31.881992, -106.642537],
        [32.000845, -106.618435],
        [32.001737, -105.761613],
        [32.760034, -105.459826],
        [32.773767, -107.775600],
        [31.785320, -107.478953],
        [31.783934, -106.529105],
    ]
    cpoints = []
    for point in points:
        cpoints.append(point[::-1])
    polygon = Polygon(cpoints)
    franklin_mountains = gpd.GeoDataFrame(index=[0],crs="EPSG:4326", geometry=[polygon])
    franklin_mountains.to_crs(epsg=32633, inplace=True)
    buffer_tx_mexico_north = gpd.overlay(buffer_tx_mexico_north,franklin_mountains,how = 'difference')
    return(buffer_tx_mexico_north)

def saveborder(data,name,setcrs):
    data = data.to_crs(epsg=4326)
#    data.set_crs(epsg=4326, inplace=True)
    if os.path.exists(name):
        if os.path.isfile(name):
            os.remove(name)
        elif os.path.isdir(name):
            shutil.rmtree(name)
    data.to_file(name)

def savehtml(geopandasdata):
#    m = folium.Map(location=[24.5024466, -102.6489334], zoom_start=6.27)
#    m = folium.Map(location=[32.5300321, -117.1014807], zoom_start=13.9)
    m = folium.Map(location=[31.6445091, -110.6722398], zoom_start=8.14)

    folium.GeoJson(geopandasdata).add_to(m)
    html_file_path = "map_with_geojson.html"
    m.save(html_file_path)

def buildbuffer(border,buffersize):
    miles_to_meters = 1609.34  # Conversion factor from miles to meters
    buffer_meters = buffersize * miles_to_meters
    buffered_gdf = border.copy()
    buffered_gdf['geometry'] = border['geometry'].buffer(buffer_meters)
    return(buffered_gdf)

def readshp(filename):
    gpddata = gpd.read_file(filename)
    gpddata = gpddata.to_crs(epsg=32633)
#    gpddata = gpddata.to_crs(epsg=4326)
    return(gpddata)

a = {
    'border_california_mexico':{'data':0,'northernbuffer':.75,'southernbuffer':50},
    'border_nm-az_mexico':{'data':0,'northernbuffer':10,'southernbuffer':50},
    'border_texas_mexico':{'data':0,'northernbuffer':50,'southernbuffer':50}}

for name,info in a.items():
    info.update({'data':readshp('lines/'+name+'.shp')})

def build_ca_mexico_buffer():
    ca_south = buildbuffer(a['border_california_mexico']['data'],a['border_california_mexico']['southernbuffer'])
    ca_north = buildbuffer(a['border_california_mexico']['data'],a['border_california_mexico']['northernbuffer'])
    usa = readshp('polygons/country_usa.shp')
    mexico = readshp('polygons/country_mexico.shp')
    buffer_ca_mexico_north = gpd.overlay(ca_north,mexico,how='difference')
    buffer_ca_mexico_south = gpd.overlay(ca_south,usa,how='difference')
    buffer_ca_mexico_south = cali_south_fixer(buffer_ca_mexico_south)
    #savehtml(buffer_ca_mexico_south)
    #saveborder(buffer_ca_mexico_south,'polygons/buffer_ca_mexico_south',False)
    buffer_ca_mexico = gpd.GeoDataFrame(pd.concat([buffer_ca_mexico_south, buffer_ca_mexico_north], ignore_index=True))
    savehtml(buffer_ca_mexico)
#    saveborder(buffer_ca_mexico,'polygons/buffer_ca_mexico',False)
    return(buffer_ca_mexico)

def test_ca_mexico():
    testpoints = {
        'north':{'coords':[32.558346, -117.030034],'boolt':False},
        'middlenorth':{'coords':[32.544962, -117.035699],'boolt':True},
        'middlesouth':{'coords':[32.531105, -117.028404],'boolt':True},
        'south':{'coords':[31.781663, -116.397143],'boolt':False}
    }

    buffer_ca_mexico = buffer_ca_mexico.to_crs(epsg=4326)
    for name,info in testpoints.items():
        point = Point(info['coords'][::-1])
    #    print(point)
    #    print(buffer_ca_mexico)
        iswithin = buffer_ca_mexico.contains(point).any()
        print('checked',iswithin,'should be',info['boolt'])

def build_nm_az_buffer():
    usa = readshp('polygons/country_usa.shp')
    mexico = readshp('polygons/country_mexico.shp')
    nmaz_south = buildbuffer(a['border_nm-az_mexico']['data'],a['border_nm-az_mexico']['southernbuffer'])
    nmaz_north = buildbuffer(a['border_nm-az_mexico']['data'],a['border_nm-az_mexico']['northernbuffer'])
    buffer_nmaz_mexico_north = gpd.overlay(nmaz_north,mexico,how='difference')
    buffer_nmaz_mexico_south = gpd.overlay(nmaz_south,usa,how = 'difference')
    buffer_nmaz_mexico_north = az_north_fixer(buffer_nmaz_mexico_north)
    buffer_nmaz_mexico = gpd.GeoDataFrame(pd.concat([buffer_nmaz_mexico_south,buffer_nmaz_mexico_north],ignore_index=True))
    savehtml(buffer_nmaz_mexico)
    saveborder(buffer_nmaz_mexico_north,'polygons/buffer_nmaz_mexico_north',False)
    saveborder(buffer_nmaz_mexico_south,'polygons/buffer_nmaz_mexico_south',False)

#    saveborder(buffer_nmaz_mexico,'polygons/buffer_nmaz_mexico',False)
    return(buffer_nmaz_mexico)

def build_tx_buffer():
    usa = readshp('polygons/country_usa.shp')
    mexico = readshp('polygons/country_mexico.shp')
    tx_south = buildbuffer(a['border_texas_mexico']['data'],a['border_texas_mexico']['southernbuffer'])
    tx_north = buildbuffer(a['border_texas_mexico']['data'],a['border_texas_mexico']['northernbuffer'])
    buffer_tx_mexico_north = gpd.overlay(tx_north,mexico,how='difference')
    buffer_tx_mexico_north = tx_north_fixer(buffer_tx_mexico_north)
    buffer_tx_mexico_south = gpd.overlay(tx_south,usa,how = 'difference')
    buffer_tx_mexico = gpd.GeoDataFrame(pd.concat([buffer_tx_mexico_south,buffer_tx_mexico_north],ignore_index=True))
    savehtml(buffer_tx_mexico)
    saveborder(buffer_tx_mexico_north,'polygons/buffer_tx_mexico_north',False)
    saveborder(buffer_tx_mexico_south,'polygons/buffer_tx_mexico_south',False)

    
    return(buffer_tx_mexico)

def all():
    c = {
        build_nm_az_buffer():['buffer_nmaz_mexico_south','buffer_nmaz_mexico_north'],
#        build_ca_mexico_buffer(),
#        build_tx_buffer()
    }
    d = gpd.GeoDataFrame(pd.concat(c,ignore_index=True))
    savehtml(d)

def savetodb(name,polygon):
    '''
    apparently the mariadb python connector doesn't support GeoDataFrame objects. 
    Instead of trying to deal with all that bullshit, I think I'm just going
    to read and write the buffers to the disk
    
    '''
    query = 'INSERT INTO geofencing.fences (name,geom) VALUES (?,ST_GeomFromText(?))'
    data = polygon['geometry'].apply(lambda x: x.wkt)
    db.execute_query(query,[name,data])

#    b = ['buffer_nmaz_mexico_south','buffer_nmaz_mexico_north']
#    a = build_nm_az_buffer()
#    for index,row in a.iterrows():
#        c = row['geometry']
#        data = [b[index],wkt.dumps(c)]


def loadfromdb(polygonname):
    query = 'SELECT ST_asText(geom) as geom_text,name FROM geofencing.fences WHERE name = ?'

    df = db.execute_query(query,[polygonname])
    df = pd.DataFrame(df)
    df['geometry'] = gpd.GeoSeries.from_wkt(df['geom_text'])

    gdf = gpd.GeoDataFrame(df, geometry='geometry')
    gdf.set_crs(epsg=32633, inplace=True)
#    savehtml(gdf)
    return(gdf)
    # holy shit this finally works


buffers = [
    'buffer_tx_mexico_north',
    'buffer_tx_mexico_south',
    'buffer_nmaz_mexico_north',
    'buffer_nmaz_mexico_south',
    'buffer_ca_mexico_north',
    'buffer_ca_mexico_south',
]


for name in buffers:
    fn = f"polygons/{name}/{name}.shp"
    bufferzone = readshp(fn)

