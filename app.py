import sqlite3
from tqdm import tqdm

from flask import Flask, request
import json 
import numpy as np
import pandas as pd

app = Flask(__name__) 

def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection

def get_station_id(station_id, conn):
    query = f"""SELECT * FROM stations WHERE station_id = {station_id}"""
    result = pd.read_sql_query(query, conn)
    return result 

def get_trip_id(trip_id, conn):
    query = f"""SELECT * FROM trips WHERE id = {trip_id}"""
    result = pd.read_sql_query(query, conn)
    return result 

@app.route('/')
def home():
    return 'Hello World'

@app.route('/stations/')
def route_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()
    
def get_all_stations(conn):
    query = f"""SELECT * FROM stations"""
    result = pd.read_sql_query(query, conn)
    return result

@app.route('/stations/<station_id>')
def route_stations_id(station_id):
    conn = make_connection()
    station = get_station_id(station_id, conn)
    return station.to_json()


@app.route('/trips/')
def route_all_trips():
    conn = make_connection()
    trips = get_all_trips(conn)
    return trips.to_json()

def get_all_trips(conn):
    # query = f"""SELECT * FROM trips limit 20"""
    query = f"""SELECT * FROM trips"""
    result = pd.read_sql_query(query, conn)
    return result

@app.route('/trips/<trips_id>')
def route_trips_id(trips_id):
    conn = make_connection()
    trips = get_trip_id(trips_id, conn)
    return trips.to_json()

@app.route('/json', methods=['POST']) 
def json_example():
    
    req = request.get_json(force=True) # Parse the incoming json data as Dictionary
    
    name = req['name']
    age = req['age']
    address = req['address']
    
    return (f'''Hello {name}, your age is {age}, and your address in {address}
            ''')



if __name__ == '__main__':
    app.run(debug=True, port=5000)

