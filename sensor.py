import paho.mqtt.client as paho                     #mqtt library
import os
import json
import random
import calendar
import time
from datetime import datetime

# generate data for testing 
# จำลองการส่ง beacon data ไปยัง mqtt broker

ACCESS_TOKEN="XhD8cD3n84r1lalo7C0x"                 #Token of your device
broker="localhost"                        #host name
port=1883                       #data listening port
beacon_id = ["E0702891-AE8E-8DC0-B0DE-3C666B326CA9:3138:0055", "FFFFFFFF-0D6C-CF10-48B9-55AAB8167ABB:F363:0055"]


def on_publish(client,userdata,result):             #create function for callback
    print("data published to thingsboard \n")
    pass
client1= paho.Client("sensor1")                     #create client object
client1.on_publish = on_publish                     #assign function to callback
client1.username_pw_set(ACCESS_TOKEN)               #access token from thingsboard device
client1.connect(broker,port,keepalive=60)           #establish connection

while True:
    date = datetime.utcnow()
    utc_time = str(calendar.timegm(date.utctimetuple()))
    rssi = str(random.randint(-100,0))
    # print(random.randint(0,1))
    payload="{"
    payload+="\"device_id\":\"5046452\","; 
    payload+="\"beacon_id\":" + "\"" + beacon_id[random.randint(0,1)] + "\"" + ",";
    payload+="\"timestamp\":" + utc_time +",";
    payload+="\"rssi\":" + rssi; 
    payload+="}"
    print("Please check LATEST TELEMETRY field of your device")
    print(payload)
    message_payload = json.dumps(payload)   # convert string to json 
    print(message_payload)
    ret= client1.publish("v1/devices/me/telemetry", message_payload) #topic-v1/devices/me/telemetry

    time.sleep(5)