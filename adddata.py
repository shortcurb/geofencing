import json, csv
import math
from support_connections.database import Database
#from support_connections.apis import Hasura
from support_connections.record import RecordFetcher
from dotenv import load_dotenv
from haversine import inverse_haversine,Unit,Direction
from shapely.wkt import loads
from shapely.geometry import Point, Polygon
from shapely.ops import unary_union
from geopy.distance import distance
from shapely.wkt import dumps


load_dotenv()
db = Database()
rec = RecordFetcher()

query = """
SELECT *
FROM (
    SELECT *, (@row_number := @row_number + 1) AS row_num
    FROM geofence , (SELECT @row_number := 0) AS rn
) AS temp_table
WHERE MOD(row_num, 2) = 0 AND border = 'US/Mexico' AND latitude >=25.7942
ORDER BY longitude asc
"""
flats = db.execute_query("SELECT * FROM geofence WHERE border = 'US/Mexico' AND latitude >=25.7942 AND longitude >= -106.995513 order by longitude asc")


#print(len(flats))

def parser(item):
    data = []
    for lvl1 in item:
        data += lvl1
#        for lvl2 in lvl1:
#            data += lvl2
    return(data)

def adddata():
    '''
    with open('mexico.json') as file:
        mexicoborders = json.load(file)
        dataloc = mexicoborders['features'][0]['geometry']['coordinates']
        print(len(dataloc))
#        flats = []
    #    print(json.dumps(dataloc,indent=2))
        metadata = []
        for item in dataloc:
    #        print(item)
            for flatitem in parser(item):
#                flats.append(flatitem)
                query = 'INSERT INTO work.geofence (latitude,longitude,border) VALUES (?,?,?)'
#                db.execute_query(query,[flatitem[1],flatitem[0],'US/Mexico'])
'''
    with open('mexico.csv','w') as file:
        a = csv.writer(file)
        a.writerow(['longitude','latitude'])
        for b in flats:
            a.writerow([b['longitude'],b['latitude']])

def smartmath():
    for i in range(1,len(flats)):
#    for i in range(1,3):

        a = flats[i-1]
        b = flats[i]
        deltalon = b['latitude']-a['latitude']
        deltalat = b['longitude']-a['longitude']
        if deltalat != 0:
            slope = (deltalon/deltalat)
            recipangle = math.tan(slope)
            print(a)
#            c = inverse_haversine([a['latitude'],a['longitude']],50,recipangle,unit=Unit.MILES)
            c = inverse_haversine([a['latitude'],a['longitude']],50,direction=Direction.NORTHEAST,unit=Unit.MILES)
            c = list(c)
            c.append('fiftymilemex')
            db.execute_query('INSERT INTO geofence (latitude,longitude,border) VALUES (?,?,?)',c)


coordinates = []
for flat in flats:
    coordinates.append([flat['longitude'],flat['latitude']])

lenc = len(coordinates)
def create_buffer(point, radius_miles):
    boundary_points = []
    for angle in range(0, 360, 20):
        endpoint = distance(miles=radius_miles).destination((point.y, point.x), angle)
        boundary_points.append((endpoint[1], endpoint[0]))
    return Polygon(boundary_points)

def createpolygon():
    buffers = []
    counter = 1
    for coord in coordinates:
        buffers.append(create_buffer(Point(coord),50))
        print(counter,'out of',lenc)
        counter += 1
    merged_polygon = unary_union(buffers)
    wkt_data = dumps(merged_polygon)
    db.execute_query("INSERT INTO geofencing.fences (name,geom) VALUES (%s, ST_GeomFromText(%s))",['testfiftymiles',wkt_data])


def testifwithin():
#    point_coords = (-100.378161, 28.667026)
    point_coords = (-104,40)
    point = Point(point_coords)
    wkt_data = db.execute_query("SELECT ST_AsGeoJSON(geom) as geom FROM geofencing.fences WHERE id=3")[0]['geom']
#    retrieved_polygon = loads(wkt_data)

    with open('polygon.geojson', 'w') as f:
        json.dump(json.loads(wkt_data), f)

#    print(retrieved_polygon)
#    is_within = retrieved_polygon.contains(point)
#    print(is_within)
#createpolygon()
testifwithin()