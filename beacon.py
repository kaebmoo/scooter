import pandas as pd
import datetime

# read beacon data
# read from database
# read from event
# read from csv

source = "C:\\Users\\CAT\\OneDrive\\Jobs\\2023\\beacon.csv"
beacon_data = pd.read_csv(source)

# convert time stamp data to integer
beacon_data["timestamp"] = beacon_data["timestamp"].astype(int)
# convert time stamp to date time
beacon_data["timestamp_datetime"] = pd.to_datetime(beacon_data["timestamp"], unit="s")
# add timezone GMT+7 Bangkok time
beacon_data["timestamp_datetime"] = beacon_data["timestamp_datetime"].dt.tz_localize('UTC').dt.tz_convert('Asia/Bangkok')
# data cleansing
beacon_data["rssi"] = beacon_data["rssi"].replace("'", "", regex=True)

print(beacon_data)
print(beacon_data["timestamp_datetime"])
beacon_report = beacon_data.groupby(["device_id", "beacon_id", "timestamp_datetime"])
print(beacon_report)
for beacon_id, frame in beacon_report:
    print(beacon_id)

beacon_data.to_csv("beacon_data.csv", index=False)