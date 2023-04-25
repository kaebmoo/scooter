import boto3
from pprint import pprint
from boto3.dynamodb.conditions import Key

def query_data(beacon):
    response = table_lastseen.query(
        KeyConditionExpression="beacon_list = :beacon",
        ExpressionAttributeValues={
        ":beacon": beacon,
        },
    )
    pprint(response["Items"])
    print(f"ScannedCount: {response['ScannedCount']}")
    return(response["Items"])

def add_data():
    pass

# Get the service resource.
dynamodb = boto3.resource('dynamodb')

# Instantiate a table resource object without actually
# creating a DynamoDB table. Note that the attributes of this table
# are lazy-loaded: a request is not made nor are the attribute
# values populated until the attributes
# on the table resource are accessed or its load() method is called.
table_scooter = dynamodb.Table('scooter')
table_lastseen = dynamodb.Table('scooter_lastseen')

# Print out some data about the table.
# This will cause a request to be made to DynamoDB and its attribute
# values will be set based on the response.
print(table_scooter.creation_date_time)
print(table_lastseen.creation_date_time)
is_in_table = query_data("E0702891-AE8E-8DC0-B0DE-3C666B326CA9:3138:0055")
print(is_in_table)
print(type(is_in_table))
if not is_in_table:
    print("not found.")