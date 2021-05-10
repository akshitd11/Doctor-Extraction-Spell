import re
import csv
import sys
import json
import time
import logging
import requests
import traceback
import mysql.connector        
from secrets import host, user, password, database   
from urllib.parse import urlencode

logging.basicConfig(filename="GeoCodeLogEntries.log", format='%(asctime)s %(message)s', filemode='a') 
    
logger=logging.getLogger() 
logger.setLevel(logging.DEBUG) 
logger.info("Setting up program")
print("Setting up program")

startTime = time.time()

try:
    mydb = mysql.connector.connect(
    host= host,
    user= user,
    password= password,
    database= database
    )

except:
    logger.error("MySql setup can't be completed " + str(sys.exc_info()[0]))
    sys.exit("MySql setup can't be completed " + str(sys.exc_info()[0]))
    
cursor = mydb.cursor()
logger.info("MySql setup completed")
print("MySql setup completed")

startTime = time.time()
invalidEntry = []
thread = []
for year in range(1960,2021,2):
    cursor.execute("SELECT address, doctor_id FROM tbl_doctors where year_info > "+ str(year) + " and year_info <= "+ str(year+1) +" ;")
    data = cursor.fetchall()

    logger.info("Doctor Information for year "+ str(year) +" to "+ str(year+1) +" extracted in " + str(time.time()-startTime))
    print("Doctor Information "+ str(year) +" to "+ str(year+1) +" extracted in " + str(time.time()-startTime))
    print(len(data))
    for i in range(len(data)):        
        try:
            id = str(data[i][1])
            logger.info("For Doctor Information: " + id )
            address = str(data[i][0])
            search = {'s':address}
            address = urlencode(search)[2:]
            url = "https://geocode.xyz/"+address+"?json=1"
            r = requests.get(url)
            content = r.json()
            latitude = content["latt"]
            longitude = content["longt"]  
            sql = 'Update tbl_doctors set `latitude` = ' + latitude + ', `longitude` = ' + longitude + ' WHERE `doctor_id` = \'' + id + '\'  ;'
            cursor.execute(sql)
            
            logger.info("Doctor Information: " + id + ' set `latitude` = ' + latitude + ', `longitude` = ' + longitude)
            mydb.commit()

        except:
            logger.error("Doctor Information: " + id + ' Error: '+ str(sys.exc_info()[0]) + ' Traceback: ' + str(traceback.print_exc()))
            print("Invalid entry:",id, "Error:", sys.exc_info()[0])
            invalidEntry.append([id])
            continue



logger.warning("Collecting all the invalid entries")
  
invalid = open('invalidEntryGeoCode.csv', 'a', newline ='') 
with invalid:     
    write = csv.writer(invalid) 
    write.writerow(['DoctorId'])
    write.writerows(invalidEntry)
             
print("Info Extracted in", time.time()-startTime)
logger.info("Doctor Information updated in " + str(time.time()-startTime))