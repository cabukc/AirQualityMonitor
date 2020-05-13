try:
    import struct
except ImportError:
    import ustruct as struct

import serial
import Adafruit_DHT
import time
import pandas as pd
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key,Attr
from boto3.dynamodb.types import TypeDeserializer
import aws
import requests
import csv

TABLE_NAME = "Air_Quality_Data"
SHORT_TABLE_NAME = "AQ_App_Data"
roomID = str('1')
CSV_NAME = "extra.csv"


class DataCollection():
    def __init__(self, roomID):
        self.roomID = roomID
        self.buffer = []
        self.uart = serial.Serial("/dev/ttyS0", baudrate=9600, timeout=0.25)
        self.DHT_SENSOR = Adafruit_DHT.DHT22
        self.DHT_PIN = 4
        self.url_switch_on  = 'http://0.0.0.0/control?cmd=GPIO,12,1'    # replace IP address
        self.url_switch_off  = 'http://0.0.0.0/control?cmd=GPIO,12,0'   # replace IP address
        self.sonoff_url        = 'NOT_INIT'

    def collectPM(self):
        for i in range(1):   #loop is only necessary so that we can use continue inside

            data = self.uart.read(32)  # read up to 32 bytes
            data = list(data)

            self.buffer += data

            while self.buffer and self.buffer[0] != 0x42:
                self.buffer.pop(0)

            if len(self.buffer) > 200:
                buffer = []  # avoid an overrun if all bad data

            if len(self.buffer) < 32:
                continue

            if self.buffer[1] != 0x4d:
                self.buffer.pop(0)
                continue

            frame = struct.unpack(">HHHHHHHHHHHHHH", bytes(self.buffer[4:]))
            
            pm10_standard, pm25_standard, pm100_standard, pm10_env, \
	        pm25_env, pm100_env, particles_03um, particles_05um, particles_10um, \
	        particles_25um, particles_50um, particles_100um, skip, checksum = frame
            
            check = sum(self.buffer[0:30])

            if check != checksum:
                self.buffer = []
                continue

            #print("Concentration Units (standard)")
            #print("---------------------------------------")
            #print("PM 1.0: %d\tPM2.5: %d\tPM10: %d" %(pm10_standard, pm25_standard, pm100_standard))

            self.buffer = self.buffer[32:]

        return pm10_standard, pm25_standard, pm100_standard

    def collectTempHum(self):
        humidity, temperature = Adafruit_DHT.read_retry(self.DHT_SENSOR, self.DHT_PIN)

        if humidity is not None and temperature is not None:
            #print("Temp={0:0.1f}*C  Humidity={1:0.1f}%".format(temperature, humidity))
            pass
        else:
            print("Failed to retrieve data from humidity sensor")

        return str(humidity), temperature

    def setOnOffFlag(self, pm10_standard, pm25_standard, pm100_standard, old_apflag):

        flag_changed = 0

        if (pm25_standard > 25 or pm100_standard > 175 ):
            apflag = str(1)
            self.sonoff_url = self.url_switch_on
            #print("Turn on air purifier")
        else:
            apflag = str(0)
            self.sonoff_url = self.url_switch_off
            #print("Turn off the air purifier")

        if (old_apflag != apflag):
            old_apflag = apflag
            flag_changed = 1
            print("Flag changed")

        return apflag, self.sonoff_url, old_apflag, flag_changed


class DynamoDB():
    def __init__(self, table_name):
    	self.table_name = table_name

    def getTable(self, table_name):
        dynamodb = aws.getResource('dynamodb', 'us-east-1')
        try:
            # Create the DynamoDB table.
            table = dynamodb.create_table(
                TableName=self.table_name,
                KeySchema=[
                    {
                        'AttributeName': 'timeStamp',
                        'KeyType': 'HASH'
                    },
                    {
	                'AttributeName': 'roomID',
			'KeyType': 'RANGE'
		    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'timeStamp',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'roomID',
                        'AttributeType': 'S'
                    },
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )

        except:
            table = dynamodb.Table(table_name)
            print("Table already exists.")

        return table

    def addToDynamo(self, timeStamp, pm10_standard, pm25_standard, pm100_standard, temperature, humidity, apflag, roomID, real_time, table_name):
        table_name.put_item(
            Item={
                'timeStamp': str(timeStamp),
                'realTimeStamp': str(real_time),
                'roomID': str(roomID),
                'pm10_standard': str(pm10_standard),
                'pm25_standard': str(pm25_standard),
                'pm100_standard': str(pm100_standard),
                'temperature': str(temperature),
                'humidity': str(humidity),
                'apflag': str(apflag)
            }
        )

class Csv():
    def __init__(self, file_name):
        self.file_name = file_name

    def writeToCsv(self, collected_data):
        with open(self.file_name, "a+", newline="\n") as fp:
            wr = csv.writer(fp)
            wr.writerow(collected_data)

if __name__ == "__main__":

    dc = DataCollection(roomID)
    db = DynamoDB(TABLE_NAME)
    main_table = db.getTable(TABLE_NAME)
    brief_table = db.getTable(SHORT_TABLE_NAME)

    c = Csv(CSV_NAME)

    old_apflag = str(0) # Used to check whether the ap_flag has changed. Only send HTTP request if changed. 

    while True:
        pm10_standard, pm25_standard, pm100_standard = dc.collectPM()	#collect PM data
        apflag, sonoff_url, old_apflag, flag_changed = dc. setOnOffFlag(pm10_standard, pm25_standard, pm100_standard, old_apflag)	#set on/off flag
        humidity, temperature = dc.collectTempHum()	#collect humidity and temperature data


        # set timeStamp
        timeStamp = time.time()
        # print("timestamp: ", t)
        meaningful_time = time.ctime(timeStamp)

        #add to main database
        db.addToDynamo(timeStamp, pm10_standard, pm25_standard, pm100_standard, temperature, humidity, apflag, roomID, meaningful_time, main_table)
        #add to brief database
        db.addToDynamo(timeStamp, pm10_standard, pm25_standard, pm100_standard, temperature, humidity, apflag, roomID, meaningful_time, brief_table)

        collected = [timeStamp, pm10_standard, pm25_standard, pm100_standard, temperature, humidity, apflag, roomID]

        c.writeToCsv(collected)

        if (flag_changed == 1 ):
            try:
                r = requests.post(sonoff_url)
                #print("Switch mode changed")
            except: #BadStatusLine:   #requests throws this error every now and then, but it's harmless in our case as we don't read any data
                pass
        # else:
            # print("Switch mode stayed the same")

        print("---------------------------------------")
        print("Current Information\n")
        print("RoomID: ", roomID)
        print("Time: ", time.ctime(timeStamp))
        print("PM1.0: ", pm10_standard)
        print("PM2.5: ", pm25_standard)
        print("PM10.0: ", pm100_standard)
        print("Temperature (C): ", temperature)
        print("Humidity (%): ", humidity)
        print("Air Purifier on/off: ", apflag)
        print("---------------------------------------")
        print("\n")

        time.sleep(30)






