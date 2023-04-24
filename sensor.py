import paho.mqtt.client as paho                     #mqtt library
import os
import json
import calendar
import time
from datetime import datetime

ACCESS_TOKEN="XhD8cD3n84r1lalo7C0x"                 #Token of your device
broker="localhost"                        #host name
port=1883                       #data listening port

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
    payload="{"
    payload+="\"device_id\":\"5046452\","; 
    payload+="\"beacon_id\":\"E0702891-AE8E-8DC0-B0DE-3C666B326CA9:3138:0055\",";
    payload+="\"timestamp\":" + utc_time +",";
    payload+="\"rssi\":-36"; 
    payload+="}"
    ret= client1.publish("v1/devices/me/telemetry",payload) #topic-v1/devices/me/telemetry
    print("Please check LATEST TELEMETRY field of your device")
    print(payload)
    time.sleep(5)