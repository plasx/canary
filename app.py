import json
import sqlite3
import time
from statistics import median, mean, mode

from flask import Flask, render_template, request, Response
from flask.json import jsonify
import numpy as np

app = Flask(__name__)


def db_setup():
    """ Setup the SQLite DB """
    conn = sqlite3.connect("database.db")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS readings (device_uuid TEXT, type TEXT, value INTEGER, date_created INTEGER)"
    )
    conn.close()


def connection():
    """ Creates SQL connection """
    if app.config["TESTING"]:
        return sqlite3.connect("test_database.db")
    else:
        return sqlite3.connect("database.db")


def generate_query(device_uuid, sensor_type, value="value", start=None, end=None):
    """ Generates query based on provided inputs """
    query = f'select {value} from readings where device_uuid="{device_uuid}" and type="{sensor_type}"'
    if None not in (start, end):
        query += f" and date_created between {start} AND {end}"
    elif start:
        query += f" and date_created >= {start}"
    elif end:
        query += f" and date_created <= {end}"
    elif not sensor_type:
        query = f'select {value} from readings where device_uuid="{device_uuid}"'
    return query


db_setup()


@app.route("/devices/<string:device_uuid>/readings/", methods=["POST", "GET"])
def request_device_readings(device_uuid):
    """
    This endpoint allows clients to POST or GET data specific sensor types.

    POST Parameters:
    * type -> The type of sensor (temperature or humidity)
    * value -> The integer value of the sensor reading
    * date_created -> The epoch date of the sensor reading.
        If none provided, we set to now.

    Optional Query Parameters:
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    * type -> The type of sensor value a client is looking for
    """

    # Set the db that we want and open the connection
    conn = connection()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    if request.method == "POST":
        # Grab the post parameters
        post_data = json.loads(request.data)
        sensor_type = post_data.get("type")
        value = post_data.get("value")
        date_created = post_data.get("date_created", int(time.time()))
        if sensor_type in ("temperature", "humidity"):
            if 0 <= value <= 100:
                # Insert data into db
                cur.execute(
                    "insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)",
                    (device_uuid, sensor_type, value, date_created),
                )

                conn.commit()

                # Return success
                return "success", 201
            else:
                return 'Please enter a value between 0 and 100 for "value"', 422
        else:
            return 'Please enter values "temperature" or "humidity" for "type"', 422
    else:
        sensor_type = request.args.get("type")
        qry = generate_query(
            device_uuid,
            sensor_type,
            "*",
            request.args.get("start"),
            request.args.get("end"),
        )
        cur.execute(qry)
        rows = cur.fetchall()

        # Return the JSON
        return (
            jsonify(
                [
                    dict(zip(["device_uuid", "type", "value", "date_created"], row))
                    for row in rows
                ]
            ),
            200,
        )


@app.route("/devices/<string:device_uuid>/readings/min/", methods=["GET"])
def request_device_readings_min(device_uuid):
    """
    This endpoint allows clients to GET the min sensor reading for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """
    sensor_type = request.args.get("type")
    if sensor_type in ("temperature", "humidity"):
        conn = connection()
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        qry = generate_query(
            device_uuid,
            sensor_type,
            "min(value)",
            request.args.get("start"),
            request.args.get("end"),
        )
        cur.execute(qry)
        rows = cur.fetchall()
        return jsonify([dict(zip(["value"], row)) for row in rows]), 200
    else:
        return 'Please enter values "temperature" or "humidity" for "type"', 422


@app.route("/devices/<string:device_uuid>/readings/max/", methods=["GET"])
def request_device_readings_max(device_uuid):
    """
    This endpoint allows clients to GET the max sensor reading for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """
    sensor_type = request.args.get("type")
    if sensor_type in ("temperature", "humidity"):
        conn = connection()
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        qry = generate_query(
            device_uuid,
            sensor_type,
            "max(value)",
            request.args.get("start"),
            request.args.get("end"),
        )
        cur.execute(qry)
        rows = cur.fetchall()
        return jsonify([dict(zip(["value"], row)) for row in rows]), 200
    else:
        return 'Please enter values "temperature" or "humidity" for "type"', 422


@app.route("/devices/<string:device_uuid>/readings/median/", methods=["GET"])
def request_device_readings_median(device_uuid):
    """
    This endpoint allows clients to GET the median sensor reading for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """
    sensor_type = request.args.get("type")
    if sensor_type in ("temperature", "humidity"):
        conn = connection()
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        qry = generate_query(
            device_uuid,
            sensor_type,
            "value",
            request.args.get("start"),
            request.args.get("end"),
        )
        cur.execute(qry)
        rows = cur.fetchall()
        median_reading = {"value": median([i["value"] for i in rows])}
        return jsonify([median_reading]), 200
    else:
        return 'Please enter values "temperature" or "humidity" for "type"', 422


@app.route("/devices/<string:device_uuid>/readings/mean/", methods=["GET"])
def request_device_readings_mean(device_uuid):
    """
    This endpoint allows clients to GET the mean sensor readings for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """
    sensor_type = request.args.get("type")
    if sensor_type in ("temperature", "humidity"):
        sensor_type = request.args.get("type")
        conn = connection()
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        qry = generate_query(
            device_uuid,
            sensor_type,
            "min(value)",
            request.args.get("start"),
            request.args.get("end"),
        )
        cur.execute(qry)
        rows = cur.fetchall()
        mean_reading = {"value": mean([i["value"] for i in rows])}
        return jsonify([mean_reading]), 200
    else:
        return 'Please enter values "temperature" or "humidity" for "type"', 422


@app.route("/devices/<string:device_uuid>/readings/mode/", methods=["GET"])
def request_device_readings_mode(device_uuid):
    """
    This endpoint allows clients to GET the mode sensor reading value for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """
    sensor_type = request.args.get("type")
    if sensor_type in ("temperature", "humidity"):
        conn = connection()
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        qry = generate_query(
            device_uuid,
            sensor_type,
            "value",
            request.args.get("start"),
            request.args.get("end"),
        )
        cur.execute(qry)
        rows = cur.fetchall()
        mode_reading = {"value": mode([i["value"] for i in rows])}
        return jsonify([mode_reading]), 200
    else:
        return 'Please enter values "temperature" or "humidity" for "type"', 422


@app.route("/devices/<string:device_uuid>/readings/quartiles/", methods=["GET"])
def request_device_readings_quartiles(device_uuid):
    """
    This endpoint allows clients to GET the 1st and 3rd quartile
    sensor reading value for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """
    sensor_type = request.args.get("type")
    if sensor_type in ("temperature", "humidity"):
        conn = connection()
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        qry = generate_query(
            device_uuid,
            sensor_type,
            "value",
            request.args.get("start"),
            request.args.get("end"),
        )
        cur.execute(qry)
        rows = cur.fetchall()
        quartile_list = sorted([i["value"] for i in rows])
        quarter1 = np.percentile(quartile_list, 25)
        quarter3 = np.percentile(quartile_list, 75)
        return jsonify({"quartile_1": quarter1, "quartile_3": quarter3}), 200
    else:
        return 'Please enter values "temperature" or "humidity" for "type"', 422


if __name__ == "__main__":
    app.run()
