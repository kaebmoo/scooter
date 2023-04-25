import paho.mqtt.client as mqtt                     #mqtt library
import os
import json
import random
import calendar
import time
from datetime import datetime
import pandas as pd

# อ่านข้อมูลจาก mqtt broker
# จำลองการรับ beacon data จาก mqtt broker

def on_message(client, userdata, message):
    # convert message payload to json object in python
    msg_payload = message.payload.decode("utf-8")
    print(type(msg_payload)) # debug message
    msg = json.loads(msg_payload)
    print(type(msg)) # debug message
    beacon_msg = json.loads(msg)
    print(type(beacon_msg)) # debug message
    print("message received " ,str(message.payload.decode("utf-8")))
    print("message topic=",message.topic)
    print("message qos=",message.qos)
    print("message retain flag=",message.retain)
    print(beacon_msg["device_id"])
    print(beacon_msg["beacon_id"])
    print(beacon_msg["timestamp"])
    print(beacon_msg["rssi"])

    if beacon_msg["rssi"] >= -70:
        is_present = True
    elif beacon_msg["rssi"] < -70:
        is_present = False 

def on_publish(client,userdata,result):             #create function for callback
    print("data published to thingsboard \n")
    pass

def is_missing():
    pass

def subscribe_data():
    ACCESS_TOKEN="XhD8cD3n84r1lalo7C0x"                 #Token of your device
    broker="localhost"                        #host name
    port=1883                       #data listening port

    client1= mqtt.Client("host1")                     #create client object
    client1.on_publish = on_publish                     #assign function to callback
    client1.on_message = on_message                     #attach function to callback
    client1.username_pw_set(ACCESS_TOKEN)               #access token from thingsboard device
    print("connecting to broker")
    client1.connect(broker,port,keepalive=60)           #establish connection
    client1.subscribe("v1/devices/me/telemetry")
    client1.loop_forever() #start the loop

station_db = pd.read_csv("station_db.csv", converters={"device_id":str})
print(station_db)
station_db["deviceid_beaconlist"] = '"'+station_db["device_id"]+'":' + station_db["beacon_list"]
print(station_db.to_json(orient='records'))

print(station_db["deviceid_beaconlist"])
beacon_list = station_db["beacon_list"].tolist()
print(beacon_list[0])
beacon_json = station_db["beacon_list"].to_json(orient='records')
print(beacon_json[0])