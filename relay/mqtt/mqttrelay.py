'''
Created on Aug 19, 2015

@author: augus
'''
import sys, datetime
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

curdb = None

def initdb():
    try:
        return MySQLdb.connect(host=mysqlhost,user=mysqluser,passwd=mysqlpass,db=mysqldbname)
    except Exception, e:
        print e
        sys.exit()

def saveTodb(db,message):
    cur = db.cursor()
    sql = "insert into mqtt_rawmessages(message,recvtime) value(%s,%s)" % (message.payload, datetime.datetime.now())
    try:
        cur.execute(sql)
    except Exception, e:
        print e 

def on_connect(client,userdata,flags,rc):
    print("Connected with result code "+str(rc))
    client.subscribe(mqttmontopic)

def on_message(client,userdata,message):
    print(message.topic+" "+str(message.payload))
    saveTodb(curdb,message)


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
        
    
