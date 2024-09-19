from flask import Flask, send_from_directory

app = Flask(__name__)

@app.route('/')
def serve_map():
    return send_from_directory('.', 'map_with_geojson.html')

if __name__ == '__main__':
#    app.run(debug=True)
    app.run(debug=False)
