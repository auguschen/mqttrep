'''
Created on Aug 19, 2015

@author: augus
'''
import sys
import MySQLdb
import paho.mqtt.client as mqtt

mqttserveraddr = "iot.darktech.org"
mqttserverport = 1883

#mqttmontopic = "$INPUT"
mqttmontopic = "tmp"

mysqlhost = "localhost"
mysqlport = 3306
mysqluser = "mqtt"
mysqlpass = "p2ssw0rd"
mysqlprefix = "mqtt_"
mysqldbname = "mqtt"
def initdb():
    try:
        return MySQLdn.connect(host=mysqlhost,user=mysqluser,passwd=mysqlpass,db=mysqldbname)
    except Exception, e:
        print e
        sys.exit()

def saveTodb(db):
    pass

def on_connect(client,userdata,flags,rc):
    print("Connected with result code "+str(rc))
    client.subscribe(mqttmontopic)

def on_message(client,userdata,message):
    print(message.topic+" "+str(message.payload))
#    saveTodb()


if __name__ == '__main__':
    print "Strating..."
    curdb = initdb();
    
    mqttclient = mqtt.Client()
    mqttclient.on_connect = on_connect
    mqttclient.on_message = on_message
    try:
        mqttclient.connect(mqttserveraddr,mqttserverport,60)
        mqttclient.loop_forever()
    except KeyboardInterrupt:
        mqttclient.disconnect()
        
    
