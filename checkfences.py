import datetime,json,pytz,os,requests,hashlib,time
from support_connections.database import Database
#from support_connections.apis import Hasura
from support_connections.record import RecordFetcher
from dotenv import load_dotenv
from haversine import haversine,Unit



load_dotenv()
db = Database()
rec = RecordFetcher()

query = """
SELECT *
FROM (
    SELECT *, (@row_number := @row_number + 1) AS row_num
    FROM geofence, (SELECT @row_number := 0) AS rn
) AS temp_table
WHERE MOD(row_num, 10) = 0 AND border = 'US/Mexico'
ORDER BY longitude asc
"""

boundaries = db.execute_query(query)
#print(len(boundaries))
#print(boundaries)

def insidefence(lrcoords):
    fencesize = 50
    close = False
#    lrcoords = (latest_record.event_data_GpsItems_Latitude,latest_record.event_data_GpsItems_Longitude)
    for coords in boundaries:
        distance = haversine([coords['latitude'],coords['longitude']],lrcoords,unit=Unit.MILES)
        if distance < fencesize:
            close = True
    return(close)

def upsertviolation(veh,latest_record):
    existing = db.execute_query("SELECT * FROM fencedvehicles WHERE vin = ?",[veh['vin']])
    if existing ==[]:
        db.execute_query("INSERT INTO fencedvehicles (vin,geofence,entered,lastchecked,currentlyviolating) VALUES (?,?,?,?,?)",[veh['vin'],'US/Mexico',latest_record.event_date,latest_record.event_date,True])
    else:
        if existing[0]['currentlyviolating'] == True:
            db.execute_query("UPDATE fencedvehicles SET lastchecked=? WHERE vin = ?",[latest_record.event_date,veh['vin']])
        else:
            db.execute_query("UPDATE fencedvehicles SET entered=?, exited=?, lastchecked=? WHERE vin = ?",[latest_record.event_date,None,latest_record.event_date,veh['vin']])

def upsertnoviolation(veh,latest_record):
    ed = latest_record.event_date
    existing = db.execute_query("SELECT * FROM fencedvehicles WHERE vin = ?",[veh['vin']])
    db.execute_query("UPDATE fencedvehicles SET exited = ?, lastchecked=?, currentlyviolating =? WHERE vin = ?",[ed,ed,False,veh['vin']])


def checkexisting():
    print('Checking existing')
    then = datetime.datetime.now()-datetime.timedelta(seconds = 30*60)
    query = 'SELECT f.vin, v.fleetnumber, v.ismi FROM fencedvehicles f INNER JOIN vehicles v  ON f.vin = v.vin WHERE f.currentlyviolating = True AND lastchecked < ?'
#    query = 'SELECT f.vin, v.fleetnumber, v.ismi FROM fencedvehicles f INNER JOIN vehicles v ON f.vin = v.vin WHERE f.currentlyviolating = True '
    checkablevehicles = db.execute_query(query,[then])
    for veh in checkablevehicles:
        latest_record = rec.get_and_parse_latest_record(veh['ismi'])
        lrcoords = (latest_record.event_data_GpsItems_Latitude,latest_record.event_data_GpsItems_Longitude)
        violation = insidefence(lrcoords)
        if violation:
            upsertviolation(veh,latest_record)
        else:
            upsertnoviolation(veh,latest_record)
    print('Done checking existing')


def checker():
    print('Checking general')
    # Select all the vehicles in TX markets that aren't known, or aren't known to be violating
    checkablevehicles = db.execute_query('SELECT v.fleetnumber,v.vin,v.ismi,f.currentlyviolating FROM vehicles v LEFT JOIN fencedvehicles f ON v.vin=f.vin WHERE v.ismi != "" AND v.decomstatus NOT LIKE "DEC6%" AND v.decomstatus NOT LIKE "DEC7%" AND (v.market = "San Antonio" OR v.market = "Austin" OR v.market = "Houston") AND (f.currentlyviolating = False OR f.currentlyviolating is Null)')
    print('checkablevehicles',len(checkablevehicles))
    counter = 1
    for veh in checkablevehicles:
#        now = datetime.datetime.now()
        print(veh)
        latest_record = rec.get_and_parse_latest_record(veh['ismi'])
        lrcoords = (latest_record.event_data_GpsItems_Latitude,latest_record.event_data_GpsItems_Longitude)
        violation = insidefence(lrcoords)
        if violation:
            upsertviolation(veh,latest_record)
        print('sleeping',counter)
        time.sleep(.5)
        counter +=1


checkexisting()
checker()





def parser(item):
    data = []
    for lvl1 in item:
        data += lvl1
#        for lvl2 in lvl1:
#            data += lvl2
    return(data)

def adddata():
    with open('mexico.json') as file:
        mexicoborders = json.load(file)
        dataloc = mexicoborders['features'][0]['geometry']['coordinates']
        print(len(dataloc))
    #    print(json.dumps(dataloc,indent=2))
        metadata = []
        for item in dataloc:
    #        print(item)
            for flatitem in parser(item):
                print(flatitem)
                query = 'INSERT INTO work.geofence (latitude,longitude,border) VALUES (?,?,?)'
                db.execute_query(query,[flatitem[1],flatitem[0],'US/Mexico'])
                