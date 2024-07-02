import folium,json
from support_connections.database import Database
from dotenv import load_dotenv
from shapely import wkb
import geojson
load_dotenv()
db = Database()

geometries = db.execute_query('SELECT name, ST_AsGeoJSON(geom) as geom FROM geofencing.fences')

def create_map(geometries):
    print(geometries)
    # Initialize map
    m = folium.Map(location=[24.5024466, -102.6489334], zoom_start=6.27)
    map = geometries[0]
#    for map in geometries[:1]:
    geom_id = map['name']
    geom_wkb = map['geom']


    # Save map to HTML file
    m.save('map_with_geojson.html')

def main():
    create_map(geometries)

if __name__ == "__main__":
    main()




'''
import folium
from support_connections.database import Database
from dotenv import load_dotenv
from shapely import wkb
import geojson
load_dotenv()
db = Database()

geoms = db.execute_query('SELECT name,ST_AsBinary(geom) as geom FROM geofencing.fences')

def create_map(geometries):
    # Initialize map
    m = folium.Map(location=[24.5024466, -102.6489334], zoom_start=6.27)

    for geom_id, geom_wkb in geometries:
        try:
            # Convert WKB to GeoJSON
            geometry = wkb.loads(geom_wkb, hex=True)
            geojson_data = geojson.Feature(geometry=geometry, properties={"id": geom_id})

            # Add GeoJSON to map with a label
            folium.GeoJson(
                geojson_data,
                tooltip=folium.GeoJsonTooltip(fields=['id'], aliases=['ID:'])
            ).add_to(m)
        except Exception as e:
            print(e)
    # Save map to HTML file
    m.save('map_with_geojson.html')

def main():
    geometries = []
    for geom in geoms:
        geometries.append([geom['name'],geom['geom']])
    create_map(geometries)
    print("Map has been created and saved as map_with_geojson.html")

if __name__ == "__main__":
    main()

    '''