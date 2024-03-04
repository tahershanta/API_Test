from fastapi import FastAPI
from datetime import date, datetime

today = date.today()

import psycopg2
import psycopg2.extras

from fastapi.responses import HTMLResponse

hostname = "lfdp.nbcc.mobi"
database = "neurodiversity_db"
username = "neurodiversity"
pwd = "neurodiversity"
port_id = "5432"

conn = None 

app = FastAPI()

@app.get('/', response_class=HTMLResponse)
async def root():
    
    return '''<html><body><div><br/><h1 style='background-color:grey; color: white;'>Welcome to the Neurodiversity project API.</h1><br/>
    <h2 style="color:slateblue;">You may access project sensor data by passing the following URLs:</h2>
    <h4><li>To get sensor data by sensor type and sensor location, pass the folloing URL:</li></h4>
    <ul>
        <a target="_blank" href="http://127.0.0.1:8000/get_data_by_sensor_type_and_location/?type=Counter&location=Room-A1018&start_date=2024-02-01&end_date=2024-03-05">
                    http://127.0.0.1:8000/get_data_by_sensor_type_and_location/?type=Counter&location=Room-A1018&start_date=2024-02-01&end_date=2024-03-05</a>
    </ul>
    <h4><li>To get sensor data by sensor type, pass the folloing URL:</li></h4>
    <ul>
        <a target="_blank" href="http://127.0.0.1:8000/get_data_by_sensor_type/?type=Counter&start_date=2024-02-01&end_date=2024-03-05">
                    http://127.0.0.1:8000/get_data_by_sensor_type/?type=Counter&start_date=2024-02-01&end_date=2024-03-05</a>
    </ul>
    <h4><li>To get sensor data by sensor location, pass the folloing URL:</li></h4>
    <ul>
        <a target="_blank" href="http://127.0.0.1:8000/get_data_by_sensor_location/?location=Room-A1018&start_date=2024-02-01&end_date=2024-03-05">
                    http://127.0.0.1:8000/get_data_by_sensor_location/?location=Room-A1018&start_date=2024-02-01&end_date=2024-03-05</a>
    </ul>
    <br/><br/><h4>Valid values for the type parameter are:</h4>
            Counter - for people counter, Illum - for illumination, or Noise - for sound level
    <br/><br/><h4>Valid values for the location parameter are:</h4>
            Room-A1018, Room-A1019, or Room-A2024</div></body></html>'''



@app.get('/get_data_by_sensor_type_and_location/')
async def get_data_by_sensor_type_and_location( type: str, location: str, start_date: str, end_date: str):
    check_result = validate_date(start_date, end_date)
    print(check_result)
    if (check_result[0]["valid"]):
        fetch_script = "select * from itemized_sensor_data WHERE sensor_type = %s AND time > %s AND time < %s AND sensor_location = %s ORDER BY sensor_name, time desc"
        fetch_values = (type, start_date, end_date, location)
        return fetch_data(fetch_script, fetch_values)
    else:
        return check_result



@app.get('/get_data_by_sensor_type/')
async def get_data_by_sensor_type_and_location( type: str, start_date: str, end_date: str):
    check_result = validate_date(start_date, end_date)
    if (check_result[0]["valid"]):
        fetch_script = "select * from itemized_sensor_data WHERE sensor_type = %s AND time > %s AND time < %s ORDER BY sensor_name, time desc"
        fetch_values = (type, start_date, end_date)
        return fetch_data(fetch_script, fetch_values)
    else:
        return check_result



@app.get('/get_data_by_sensor_location/')
async def get_data_by_sensor_location(location: str, start_date: str, end_date: str):
    check_result = validate_date(start_date, end_date)
    if (check_result[0]["valid"]):
        fetch_script = "select * from itemized_sensor_data WHERE time > %s AND time < %s AND sensor_location = %s ORDER BY sensor_name, time desc"
        fetch_values = (start_date, end_date, location)
        return fetch_data(fetch_script, fetch_values)
    else:
        return check_result



def validate_date(start_date: str, end_date: str):
    if (datetime.strptime(start_date, '%Y-%m-%d').date() > today):
        return [{"valid": False, "msg": "Error! Start date cannot be > today's date!"}]
    elif (datetime.strptime(end_date, '%Y-%m-%d').date() < datetime.strptime(start_date, '%Y-%m-%d').date()):
        return [{"valid": False, "msg": "Error! End date cannot be < start date!"}]
    else:
        return [{"valid": True, "msg": "success"}]
    


def fetch_data(fetch_script: str, fetch_values: str):
    try:
        with psycopg2.connect(
            host = hostname,
            dbname = database,
            user = username,
            password = pwd,
            port = port_id,
            sslmode= 'disable'
            
        )   as conn:

            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(fetch_script, fetch_values)
                seq = 0
                data = {}
                data_array = []

                for record in cur.fetchall():
                    seq = seq + 1
                    data = {"seq": seq, "sensor_eui": record["sensor_eui"], "time": record["time"], "sensor_Type": record["sensor_type"], "sensor_name": record["sensor_name"], "sensor_location": record["sensor_location"]}
                    if record["sensor_type"] == "Counter":
                        data["total_in"]= record["data_json"]["total_counter_a"]
                        data["total_out"]= record["data_json"]["total_counter_b"]
                        data["inside"] = record["data_json"]["total_counter_a"] - record["data_json"]["total_counter_b"]
                    elif record["sensor_type"] == "Illum":
                        data["temp"]= record["data_json"]["TempC_SHT"]
                        data["rh"]= record["data_json"]["Hum_SHT"]
                        data["illum"] = record["data_json"]["ILL_lx"]
                    elif record["sensor_type"] == "Noise":
                        data["la"]= record["data_json"]["la"]
                        data["laeq"]= record["data_json"]["laeq"]
                        data["lamax"] = record["data_json"]["lamax"]
                    
                    data_array.append(data)

                return data_array

    except Exception as error:
        return error
    finally:
        if conn is not None:
            conn.close()
