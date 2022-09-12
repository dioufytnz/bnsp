import paho.mqtt.client as mqttClient
import time
import json
import requests
import os.path
import subprocess
import logging

logging.basicConfig(filename='/projets/bnsp/pompe1_2.log', level=logging.DEBUG)

def on_connect(client, userdata, flags, rc):

    if rc == 0:

        print("Connected to broker")
        logging.info('Connected to broker') ###

        global Connected                #Use global variable
        Connected = True                #Signal connection

    else:

        print("Connection failed")
        logging.warning('Connection failed') ###

def on_message(client, userdata, message): #FONCTION UTILISE LORS DE LA RECEPTION D'UN MESSAGE
    print (message.payload)
    logging.info('%s', message.payload) ###
    message.payload = message.payload.decode("utf-8")
    with open("/projets/bnsp/pompe1_2.txt","w") as f:
         f.write(message.payload)
    with open('/projets/bnsp/pompe1_2.txt') as f:
        data = json.load(f)
    #print(data['sensorDatas'][23]['flag'])
    #print(data['sensorDatas'][23]['value'])
    DI1=int(data['sensorDatas'][0]['switcher'])
    DI2=int(data['sensorDatas'][1]['switcher'])
    DI3=int(data['sensorDatas'][2]['switcher'])
    DI4=int(data['sensorDatas'][3]['switcher'])
    DI5=int(data['sensorDatas'][4]['switcher'])

    fn1 = '/projets/bnsp/pompe1.io'
    if DI5 == 1:
        print("POMPE 1 UP")
        logging.info('POMPE 1 UP')
        if not os.path.exists(fn1):
            open(fn1, 'w').close()
    else:
        if os.path.exists(fn1):
            os.remove(fn1)
        print("POMPE 1 DOWN")
        logging.info('POMPE 1 DOWN')

    DI6=int(data['sensorDatas'][5]['switcher'])
    DI7=int(data['sensorDatas'][6]['switcher'])

    fn2 = '/projets/bnsp/pompe2.io'
    if DI7 == 1:
        print ("POMPE 2 UP")
        logging.info('POMPE 2 UP') ###
        if not os.path.exists(fn2):
            open(fn2, 'w').close()
    else:
        if os.path.exists(fn2):
            os.remove(fn2)
        print("POMPE 2 DOWN")
        logging.info('POMPE 2 DOWN') ###

    DI8=int(data['sensorDatas'][7]['switcher'])

    AI1=float(data['sensorDatas'][16]['value'])
    voltage1_check = subprocess.check_output(["sh", "/projets/bnsp/influxdb_query.sh", "pompe_1", "voltage"])
    voltage1_check = voltage1_check.decode("utf-8")
    voltage1_check = voltage1_check.strip()
    if voltage1_check == "NA":
        voltage1_check = 1.0
        print("PAS DE DONNEES SUR POMPE1 VOLT 30MN : 1 IN USE")
        logging.warning('PAS DE DONNEES SUR POMPE1 VOLT 30MN : 1 IN USE') ###
    AI1_check = subprocess.check_output(["sh", "/projets/bnsp/influxdb_query.sh", "pompe_1", "AI1"])
    AI1_check = AI1_check.decode("utf-8")
    AI1_check = AI1_check.strip()
    if AI1_check == "NA":
        AI1_check = AI1
        print("PAS DE DONNEES SUR POMPE1 AMP 30MN : AI1 IN USE")
        logging.warning('PAS DE DONNEES SUR POMPE1 AMP 30MN : AI1 IN USE') ###
    AI1_check = float(AI1_check)
    if AI1_check < AI1:
        AI1_check = AI1
    voltage1_check = float(voltage1_check)
    PWR1 = 1.732 * AI1_check * voltage1_check
    print("AI1 : ", AI1)
    logging.info('AI1 : %s', AI1) ###
    print("AI1_check : ", AI1_check)
    logging.info('AI1_check : %s', AI1_check) ###
    print("voltage1_check : ", voltage1_check)
    logging.info('voltage1_check : %s', voltage1_check) ###
    print("PWR1 : ", PWR1)
    logging.info('PWR1 : %s', PWR1) ###

    AI2=float(data['sensorDatas'][17]['value'])
    AI2_check = subprocess.check_output(["sh", "/projets/bnsp/influxdb_query.sh", "pompe_1", "AI2"])
    AI2_check = AI2_check.decode("utf-8")
    AI2_check = AI2_check.strip()
    if AI2_check == "NA":
        AI2_check = AI2
        print("PAS DE DONNEES SUR POMPE1 AMP 30MN : AI2 IN USE")
        logging.warning('PAS DE DONNEES SUR POMPE1 AMP 30MN : AI2 IN USE') ###
    AI2_check = float(AI2_check)
    if AI2_check < AI2:
        AI2_check = AI2
    print("AI2 : ", AI2)
    logging.info('AI2 : %s', AI2) ###
    print("AI2_check : ", AI2_check)
    logging.info('AI2_check : %s', AI2_check) ###

    AI3=float(data['sensorDatas'][18]['value'])
    AI3_check = subprocess.check_output(["sh", "/projets/bnsp/influxdb_query.sh", "pompe_1", "AI3"])
    AI3_check = AI3_check.decode("utf-8")
    AI3_check = AI3_check.strip()
    if AI3_check == "NA":
        AI3_check = AI3
        print("PAS DE DONNEES SUR POMPE1 AMP 30MN : AI3 IN USE")
        logging.warning('PAS DE DONNEES SUR POMPE1 AMP 30MN : AI3 IN USE') ###
    AI3_check = float(AI3_check)
    if AI3_check < AI3:
        AI3_check = AI3
    print("AI3 : ", AI3)
    logging.info('AI3 : %s', AI3) ###
    print("AI3_check : ", AI3_check)
    logging.info('AI3_check : %s', AI3_check) ###

    AI4=float(data['sensorDatas'][19]['value'])
    AI4_check = subprocess.check_output(
        ["sh", "/projets/bnsp/influxdb_query.sh", "pompe_1", "AI4"])
    AI4_check = AI4_check.decode("utf-8")
    AI4_check = AI4_check.strip()
    if AI4_check == "NA":
        AI4_check = AI4
        print("PAS DE DONNEES SUR POMPE1 AMP 30MN : AI4 IN USE")
        logging.warning('PAS DE DONNEES SUR POMPE1 AMP 30MN : AI4 IN USE')
    AI4_check = float(AI4_check)
    if AI4_check < AI4:
        AI4_check = AI4
    print("AI4 : ", AI4)
    logging.info('AI4 : %s', AI4)
    print("AI4_check : ", AI4_check)
    logging.info('AI4_check : %s', AI4_check)

    AI5=float(data['sensorDatas'][20]['value'])
    voltage5_check = subprocess.check_output(["sh", "/projets/bnsp/influxdb_query.sh", "pompe_2", "voltage"])
    voltage5_check = voltage5_check.decode("utf-8")
    voltage5_check = voltage5_check.strip()
    if voltage5_check == "NA":
        voltage5_check = 1.0
        print("PAS DE DONNEES SUR POMPE2 VOLT 30MN : 1 IN USE")
        logging.warning('PAS DE DONNEES SUR POMPE1 VOLT 30MN : 1 IN USE') ###
    AI5_check = subprocess.check_output(["sh", "/projets/bnsp/influxdb_query.sh", "pompe_2", "AI5"])
    AI5_check = AI5_check.decode("utf-8")
    AI5_check = AI5_check.strip()
    if AI5_check == "NA":
        AI5_check = AI5
        print("PAS DE DONNEES SUR POMPE2 AMP 30MN : AI5 IN USE")
        logging.warning('PAS DE DONNEES SUR POMPE1 AMP 30MN : AI5 IN USE') ###
    AI5_check = float(AI5_check)
    if AI5_check < AI5:
        AI5_check = AI5
    voltage5_check = float(voltage5_check)
    PWR2 = 1.732 * AI5_check * voltage5_check
    print("AI5 : ", AI5)
    logging.info('AI5 : %s', AI5) ###
    print("AI5_check : ", AI5_check)
    logging.info('AI5_check : %s', AI5_check) ###
    print("voltage5_check : ", voltage5_check)
    logging.info('voltage5_check : %s', voltage5_check) ###
    print("PWR2 : ", PWR2)
    logging.info('PWR2 : %s', PWR2) ###

    AI6=float(data['sensorDatas'][21]['value'])
    AI6_check = subprocess.check_output(["sh", "/projets/bnsp/influxdb_query.sh", "pompe_2", "AI6"])
    AI6_check = AI6_check.decode("utf-8")
    AI6_check = AI6_check.strip()
    if AI6_check == "NA":
        AI6_check = AI6
        print("PAS DE DONNEES SUR POMPE2 AMP 30MN : AI6 IN USE")
    AI6_check = float(AI6_check)
    if AI6_check < AI6:
        AI6_check = AI6
    print("AI6 : ", AI6)
    logging.info('AI6 : %s', AI6) ###
    print("AI6_check : ", AI6_check)
    logging.info('AI6_check : %s', AI6_check) ###

    AI7=float(data['sensorDatas'][21]['value'])
    AI7_check = subprocess.check_output(["sh", "/projets/bnsp/influxdb_query.sh", "pompe_1", "AI7"])
    AI7_check = AI7_check.decode("utf-8")
    AI7_check = AI7_check.strip()
    if AI7_check == "NA":
        AI7_check = AI7
        print("PAS DE DONNEES SUR POMPE2 AMP 30MN : AI7 P1 IN USE")
    AI7_check = float(AI7_check)
    if AI7_check < AI7:
        AI7_check = AI7
    print("AI7 : ", AI7)
    logging.info('AI7 : %s', AI4) ###
    print("AI7_check : ", AI7_check)
    logging.info('AI7_check : %s', AI7_check) ###

    AI8=float(data['sensorDatas'][23]['value'])

    if DI5 == 0 and DI7 == 1:
        AI5_check = AI4_check
        AI6_check = AI2_check
        AI7_check = AI3_check
        AI1_check = 0.0
        AI2_check = 0.0
        AI3_check = 0.0
        PWR2 = PWR1
        PWR1 = 0.0

    if DI7 == 0 and DI5 == 1:
        AI1_check = AI4_check
        AI2_check = AI2_check
        AI3_check = AI3_check
        AI5_check = 0.0
        AI6_check = 0.0
        AI7_check = 0.0
        PWR1 = PWR1
        PWR2 = 0.0

e




    url_string = 'http://localhost:48086/write?db=bnsp'

    #data_string1 = "pompe_1,location=camille_basse DI1={},DI2={},DI3={},DI4={},AI1={},AI2={},AI3={},AI4={},PWR1={}".format(DI1,DI2,DI3,DI4,AI1,AI2,AI3,AI4,PWR1)
    #data_string2 = "pompe_2,location=camille_basse DI5={},DI6={},DI7={},DI8={},AI5={},AI6={},AI7={},AI8={},PWR2={}".format(DI5,DI6,DI7,DI8,AI5,AI6,AI7,AI8,PWR2)
    data_string1 = "pompe_1,location=camille_basse DI5={},DI1={},DI3={},DI4={},AI1={},AI2={},AI3={},AI4={},PWR1={}".format(DI5,DI1,DI3,DI4,AI1_check,AI2_check,AI3_check,AI4,PWR1)
    data_string2 = "pompe_2,location=camille_basse DI2={},DI6={},DI7={},DI8={},AI5={},AI6={},AI7={},AI8={},PWR2={}".format(DI2,DI6,DI7,DI8,AI5_check,AI6_check,AI7_check,AI8,PWR2)

    r1 = requests.post(url_string, data=data_string1)
    r2 = requests.post(url_string, data=data_string2)



Connected = False   #global variable for the state of the connection

broker_address= "127.0.0.1"  #Broker address
port = 1883                         #Broker port
user = ""                    #Connection username
password = ""            #Connection password

client = mqttClient.Client("Python")               #create new instance
client.username_pw_set(user, password=password)    #set username and password
client.on_connect= on_connect                      #attach function to callback
client.on_message= on_message                      #attach function to callback
client.connect(broker_address,port,60) #connect
client.subscribe("pompe_1_2") #subscribe
client.loop_forever() #then keep listening forevers