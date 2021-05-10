import re
import csv
import sys
import json
import time
import logging
import requests
import traceback
import mysql.connector
from geopy.geocoders import Nominatim            
from secrets import host, user, password, database   

startTime = time.time()

mydb = mysql.connector.connect(
    host= host,
    user= user,
    password= password,
    database= database
)
    
cursor = mydb.cursor()
print("MySql setup completed")

geolocator = Nominatim(user_agent="SarasviAppDatabase")
            
cursor.execute("SELECT address,doctor_id FROM tbl_doctors where not address = null or not address = 'null' limit 10;")

data = cursor.fetchall()
print(geolocator)
for i in range(0,10):    
    address = data[i][0]
    id = str(data[i][1])
    print(id,address)
    try:
        location = geolocator.geocode(address)
        print(location)
        sql = 'Update tbl_doctors set `latitude` = ' + str(location.latitude) + ', `longitude` = ' + str(location.longitude) + 'WHERE `MCIdoctorId` = \'' + id + '\'  ;'
        print(sql)
        #cursor.execute(sql)
        #mydb.commit()
    except:
        print(address)
