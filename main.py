import re
import csv
import sys
import json
import time
import requests
import traceback
import logging
import mysql.connector
from threading import Lock,Thread
from secrets import host, user, password, database   
from guess_indian_gender import IndianGenderPredictor

logging.basicConfig(filename="logEntries5.log", format='%(asctime)s %(message)s', filemode='a') 
    
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
start = 0

predict = IndianGenderPredictor()   
for year in range(1960,2021,2):
    cursor.execute("SELECT firstName,doctorId,registrationNo,status,addlqual1,addlqualuniv1,addlqualyear1,addlqual2,addlqualuniv2,addlqualyear2,addlqual3,addlqualuniv3,addlqualyear3 FROM Doctor_Info_Tbl where status = 1 and yearInfo > "+ str(year) + " and yearInfo <= "+ str(year+1) +" ;")
    data = cursor.fetchall()

    logger.info("Doctor Information for year "+ str(year) +" to "+ str(year+1) +" extracted in " + str(time.time()-startTime))
    print("Doctor Information "+ str(year) +" to "+ str(year+1) +" extracted in " + str(time.time()-startTime))
    print(len(data))
    for i in range(len(data)):    
        if i % 10000 == 0:
            print(i, "Done")
            logger.info("Currently at i = " + str(i))

        try:
            id = str(data[i][1])
            regNo = str(data[i][2])
            
            gender = ''

            if 'Miss' in data[i][0] or 'Mrs.' in data[i][0] or 'Ms.' in data[i][0] :
                gender = 'Female'
            elif 'Mr.' in data[i][0] or 'Master' in data[i][0]:
                gender = 'Male'
            else:
                name = str(data[i][0])
                name = name[2:-4]
                firstName = name.split(" ")[0]
                gender = predict.predict(name=firstName)
                gender = gender.capitalize()
            
            logger.info("For Doctor Information: " + id + ' gender = ' + gender)
            
            with Lock():
                addlQual = {
                    '1' : {
                        'qual' : data[i][4],
                        'university': data[i][5],
                        'year': data[i][6]
                    },
                    '2' : {
                        'qual' : data[i][7],
                        'university': data[i][8],
                        'year': data[i][9]
                    },
                    '3' : {
                        'qual' : data[i][10],
                        'university': data[i][11],
                        'year': data[i][12]
                    }
                }
                sql = 'Update tbl_doctors set `gender` = \'' + gender +'\', `addl_qual` = \'' + json.dumps(addlQual) + '\' WHERE `mci_doctor_id` = \'' + id + '\' and `registration_number` = \'' + regNo + '\' and doctor_id > 0;'
                
                #print(sql)
                cursor.execute(sql)
                mydb.commit()
                
                sql = 'Update Doctor_Info_Tbl set `status` = 2 WHERE `doctorId` = \'' + id + '\' and `registrationNo` = \'' + regNo + '\' ;'
                
                cursor.execute(sql)
                
                logger.info("Doctor Information: " + id + ' Committing... ')
                mydb.commit()
                
        except:
            logger.error("Doctor Information: " + id + ' Error: '+ str(sys.exc_info()[0]) + ' Traceback: ' + str(traceback.print_exc()))
            print("Invalid entry:",id, "Error:", sys.exc_info()[0])
            invalidEntry.append([id,regNo])
            continue


logger.warning("Collecting all the invalid entries")
  
invalid = open('invalidEntry.csv', 'a', newline ='') 
with invalid:     
    write = csv.writer(invalid) 
    write.writerow(['DoctorId','RegNo'])
    write.writerows(invalidEntry)
             
print("Info Extracted in", time.time()-startTime)
logger.info("Doctor Information updated in " + str(time.time()-startTime))