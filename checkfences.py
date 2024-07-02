
from support_connections.database import Database,GeoDB
from support_connections.record import RecordFetcher,FluidFleetRecords
from support_connections.identifiers import Identify
from dotenv import load_dotenv
from shapely.geometry import Point
from support_connections.apis import FluidFleet
import datetime,json,time

buffers = {
    'buffer_tx_mexico_north':0,
    'buffer_tx_mexico_south':0,
    'buffer_nmaz_mexico_north':0,
    'buffer_nmaz_mexico_south':0,
    'buffer_ca_mexico_north':0,
    'buffer_ca_mexico_south':0,
}


class Fencer:
    def __init__(self):
        load_dotenv()
        self.db = Database()
        #rec = RecordFetcher()
        self.ff = FluidFleet()
        gdb = GeoDB('geofencing')
        self.id = Identify()
        self.buffers = gdb.select_gdf('SELECT name,geom FROM geofencing.fences WHERE groupname LIKE "buffer%"')


    def checkbuffers(self,lat,lon):
        point = Point(lon,lat)
        withins = []
        containing_polygons = self.buffers[self.buffers.contains(point)]
        if not containing_polygons.empty:
            withins.append((point, containing_polygons))
        return(withins)

    def checkfences(self):
        now = datetime.datetime.now()
        then = now - datetime.timedelta(seconds = 60)
#        now = datetime.datetime.strftime(now,'%Y-%m-%dT%H:%M:%S.00Z')
#        then = datetime.datetime.strftime(then,'%Y-%m-%dT%H:%M:%S.00Z')

        records = FluidFleetRecords().get_and_parse_ff_records(then,now,limit=10000)

        for rec in records:
            lat = rec.coordinates_x
            lon = rec.coordinates_y
            withins = self.checkbuffers(lat,lon)
            if withins != []:
                vd = self.id.lookup_known_ID(rec.ismi,'ismi',self.db,)
                print(rec)
                print(vd)

#                print(f"https://www.google.com/maps/place/{lat},{lon}")
#                print(withins)



Fencer().checkfences()
#other()



'''
{
  "ismi": 310170862134029,
  "lat": "37.227768",
  "lng": "-76.67811",
  "odometer_LVCAN": 16352.044937676315,
  "odometer_continuous": 4.113487516621721,
  "event_date": "07-02-2024 20:26:21",
  "last-data-event": "07-02-2024 20:26:24",
  "vehicle_battery_level": 13889,
  "device_battery_level": "3991",
  "fuel_level": 45,
  "vehicle_type": 1,
  "speed": 32,
  "OverSpeeding": 0,
  "reliability_status": ""
}

'''