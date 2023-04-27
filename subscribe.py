import paho.mqtt.client as mqtt                     #mqtt library
import os
import json
import random
import calendar
import time
from datetime import datetime
import pandas as pd
import boto3
from pprint import pprint
from boto3.dynamodb.conditions import Key
from decimal import Decimal

# decode json result from dynamodb aws, decimal(0) text 
class DecimalEncoder(json.JSONEncoder):
  def default(self, obj):
    if isinstance(obj, Decimal):
      return str(obj)
    return json.JSONEncoder.default(self, obj)

# set up dynamodb and table
# Get the service resource.
dynamodb = boto3.resource('dynamodb')

# Instantiate a table resource object without actually
# creating a DynamoDB table. Note that the attributes of this table
# are lazy-loaded: a request is not made nor are the attribute
# values populated until the attributes
# on the table resource are accessed or its load() method is called.
table_scooter = dynamodb.Table('scooter')
table_lastseen = dynamodb.Table('scooter_lastseen')
table_transaction = dynamodb.Table('scooter_transaction')

def query_data(beacon):
    print("searching for ", beacon)
    response = table_lastseen.query(
        KeyConditionExpression="lastseen = :beacon",
        ExpressionAttributeValues={
        ":beacon": beacon,
        },
    )
    # pprint(response["Items"])
    # print(f"ScannedCount: {response['ScannedCount']}")
    return(response["Items"])

def add_lastseen_data(items):
    print("add: ", items)
    response = table_lastseen.put_item(Item = items)

def update_lastseen_data(items):
    print("update: ", items)
    table_lastseen.update_item(
    Key={
        #'beacon_id': items["beacon_id"],
        'lastseen': items["beacon_id"]
    },
    UpdateExpression='SET device_id = :device_id, #ts = :timestamp, rssi = :rssi, is_present = :is_present, #st = :state',
    ExpressionAttributeValues={
        ':device_id': items["device_id"],
        ':timestamp': items["timestamp"],
        ':rssi': items["rssi"],
        ':is_present': items["is_present"],
        ':state': items["state"]
    },
    ExpressionAttributeNames={
        "#ts": "timestamp",
        "#st": "state"
    }
)


# อ่านข้อมูลจาก mqtt broker
# จำลองการรับ beacon data จาก mqtt broker

def on_message(client, userdata, message):
    # convert message payload to json object in python
    msg_payload = message.payload.decode("utf-8")
    # print(type(msg_payload)) # debug message
    msg = json.loads(msg_payload)
    # print(type(msg)) # debug message
    beacon_msg = json.loads(msg)
    # print(type(beacon_msg)) # debug message
    '''
    print("message received " ,str(message.payload.decode("utf-8")))
    print("message topic=",message.topic)
    print("message qos=",message.qos)
    print("message retain flag=",message.retain)
    '''
    '''
    print(beacon_msg["device_id"])
    print(beacon_msg["beacon_id"])
    print(beacon_msg["timestamp"])
    print(beacon_msg["rssi"])
    '''
    if beacon_msg["rssi"] >= -70:
        is_present = True
    elif beacon_msg["rssi"] < -70:
        is_present = False 
    beacon_msg['is_present'] = is_present
    beacon_msg['lastseen'] = beacon_msg["beacon_id"]
    # beacon_msg['lastseen'] = str(int(time.time()))
    # beacon_msg["rssi"] = str(beacon_msg["rssi"])
    print(json.dumps(beacon_msg, indent=4))
    is_in_table = query_data(beacon_msg["beacon_id"]) # query beacon_id from lastseen table

    # if not present in table 
    if not is_in_table:
        # new data
        print("beacon_id not present in lastseen table. adding data")
        beacon_msg['state'] = 0
        # add data to last seen table
        response = table_lastseen.put_item(Item = beacon_msg)
    else:
        json_data = json.dumps(is_in_table, cls=DecimalEncoder)
        lastseen_data = json.loads(json_data)
        print("is present: ", lastseen_data[0]["is_present"])
        print("state: ", lastseen_data[0]["state"])
        state = int(lastseen_data[0]["state"])
        print("beacon found")
        # print(is_in_table)
        # compare incoming message (is_present) == lastseen (is_present)
        # 
        if (beacon_msg["is_present"] == lastseen_data[0]["is_present"]):
            # update lastseen
            if (lastseen_data[0]["is_present"] == False):
                if state > 0: # second false, F->F เกิด False สองครั้ง
                    beacon_msg['state'] = 0
                    update_lastseen_data(beacon_msg) # update last seen table 

                    # add record to transaction table
                    
                    beacon_msg.pop('state', None)
                    if beacon_msg["is_present"] == True:
                        beacon_msg['status'] = "completed"
                    else:
                        beacon_msg['status'] = "ongoing"
                    print("ongoin, add transaction")
                    response = table_transaction.put_item(Item = beacon_msg)
                '''
                else:
                    beacon_msg['state'] = 0
                    update_lastseen_data(beacon_msg)
                '''
            # true
            beacon_msg['state'] = 0
            update_lastseen_data(beacon_msg)
        else:
            # status change
            # change state from false -> true
            if (beacon_msg["is_present"] == True) and (lastseen_data[0]["is_present"] == False):
                # completed
                beacon_msg['state'] = 0
                update_lastseen_data(beacon_msg)
                # add record to transaction table
                
                beacon_msg.pop('state', None)
                if beacon_msg["is_present"] == True:
                    beacon_msg['status'] = "completed"
                else:
                    beacon_msg['status'] = "ongoing"
                if state == 0: # in case F->T->F->T->F-T เกิด Flase สลับ True
                    print("completed, add transaction.")
                    response = table_transaction.put_item(Item = beacon_msg)
            # change state from true (present) -> false (ongoing)
            elif (beacon_msg["is_present"] == False) and (lastseen_data[0]["is_present"] == True):
                if (state < 1): # first false
                    beacon_msg['state'] = state + 1
                    update_lastseen_data(beacon_msg)
                

            

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

def beacon_list(device_id):
    station_db = pd.read_csv("station_db.csv", converters={"device_id":str})
    # print(station_db)

    row_index = station_db.index[station_db['device_id'] == device_id].tolist()
    # print(row_index) return row index for specific device_id
    json_object = station_db.loc[row_index, "beacon_list"]
    # print(json_object)

    # beacon list 
    for device_id in json_object:
        device = json.loads(device_id)
        device_list = []
        for device_element in device:
            # print(device_element['S'])
            device_list.append(device_element['S'])

    return(device_list)


def main():
    print()
    print("query beacon list by station id", beacon_list("5046452"))

    subscribe_data()

if __name__ == "__main__":
    main()
