from crypt import methods
from flask import Flask, request
import numpy as np
import pandas as pd
import sqlite3

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

def get_trip_id_bike(bike_id, conn):
    query = f"""SELECT bikeid, AVG(duration_minutes) as av_per_bike FROM trips WHERE bikeid = {bike_id}"""
    result = pd.read_sql_query(query, conn)
    return result 

def insert_into_stations(data, conn):
    query = f"""INSERT INTO stations values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

def insert_into_trip(data, conn):
    query = f"""INSERT INTO trips values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

def get_all_stations(conn):
    query = f"""SELECT * FROM stations"""
    result = pd.read_sql_query(query, conn)
    return result
    
def get_all_trips(conn):
    query = f"""SELECT * FROM trips"""
    result = pd.read_sql_query(query, conn)
    return result


## fungsi global ----------------------------------  
    
@app.route('/')
def home():
    return 'Hello World'

@app.route('/stations/')
def route_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()
    
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

@app.route('/stations/add', methods=['POST']) 
def route_add_station():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    
    conn = make_connection()
    result = insert_into_stations(data, conn)
    return result

@app.route('/trips/add', methods=['POST']) 
def route_add_trip():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    
    conn = make_connection()
    result = insert_into_trip(data, conn)
    return result

@app.route('/trips/average_duration') 
def route_avg_trip():
    # parse and transform incoming data into a tuple as we need 
    conn = make_connection()
    query = f"""
            SELECT * 
            FROM trips
            LIMIT 100
            """
    average_res = pd.read_sql_query(query, conn)
    result = average_res.groupby(['start_station_name','end_station_name']).mean()['duration_minutes']
    return result.to_json()

@app.route('/trips/average_duration/<bike_id>')
def route_avg_trip_per_bike(bike_id):
    conn = make_connection()
    # query = f"""
    #         SELECT bikeid, AVG(duration_minutes) 
    #         FROM trips
    #         WHERE bikeid = {bikeid}
    #         """
    # average_res_bike = pd.read_sql_query(query, conn)
    average_res_bike = get_trip_id_bike(bike_id,conn)
    return average_res_bike.to_json()

@app.route('/trips/station_trend', methods=['POST'])
def station_trend():
    input_data = request.get_json() # Get the input as dictionary
    specified_date = input_data['year_month'] # Select specific items (period) from the dictionary (the value will be "2015-08")
    start_station_id = input_data['start_station_id'] # Select specific items (period) 

    # Subset the data with query 
    conn = make_connection()
    query = f"SELECT * FROM trips WHERE start_time LIKE '{specified_date}%' AND start_station_id={start_station_id}"
    selected_data = pd.read_sql_query(query, conn)

    # Make the aggregate
    result = selected_data.groupby('bikeid').agg({
        'duration_minutes' : 'mean'
    })

    # Return the result
    return result.to_json()




if __name__ == '__main__':
    app.run(debug=True, port=5000)

