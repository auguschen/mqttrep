'''
Created on Aug 19, 2015

@author: augus
'''
#-*- coding: utf-8 -*-

import sys, MySQLdb, json
import paho.mqtt.client as mqtt
from DBUtils.PooledDB import PooledDB

DEBUG = True

SAVETODB = True

mqtt_serveraddr = "localhost"
mqtt_serverport = 14998
mqtt_client_id = "client_relay"
mqtt_montopic = "$INPUT"
mqtt_qos = 2
mysqlhost = "localhost"
mysqlport = 3306
mysqluser = "mqtt"
mysqlpass = "p2ssw0rd"
mysqlprefix = "mqtt_"
mysqldbname = "mqtt"

pool = None

def initPool():
    try:
        return PooledDB(MySQLdb,10,50,50,100,False,host=mysqlhost,user=mysqluser,passwd=mysqlpass,db=mysqldbname,charset='utf8') 
    except Exception, e:
        print e
        sys.exit()
    
def initdb():
    try:
        return MySQLdb.connect(host=mysqlhost,user=mysqluser,passwd=mysqlpass,db=mysqldbname,charset="utf8")
    except Exception, e:
        print e
        sys.exit()

def saveTodb(message):
    db = pool.connection()
    cur = db.cursor()
    jsonObj = json.loads(message.payload)
    client_id = jsonObj["client_id"]
    message = jsonObj["message"]
    topic = jsonObj["topic"]
    message_datetime = jsonObj["message_datetime"]
    
    sql = "insert into mqtt_messages(client_id, payload,topic,message_datetime) value('%s', '%s', '%s', '%s')" % (client_id, message, topic, message_datetime)
    try:
        cur.execute(sql)
        db.commit()
        return True
    except Exception, e:
        print e 
        db.rollback()
        return False
    cur.close()
    db.close()
    
def on_connect(client,userdata,flags,rc):
    if (DEBUG): 
        print("Connected with result code "+str(rc))
    client.subscribe(mqtt_montopic+"/#", mqtt_qos)

def on_message(client,userdata,message):
    if (DEBUG): 
        print(message.topic+" "+ message.payload.decode('raw_unicode_escape'))
    if (SAVETODB):
        if (saveTodb(message)):
            republish(client, message)

def republish(client, message):
    jsonObj = json.loads(message.payload)
    client_id = jsonObj["client_id"]
    message = jsonObj["message"]
    topic = jsonObj["topic"]
    message_datetime = jsonObj["message_datetime"]
    client.publish(topic, message, mqtt_qos)

if __name__ == '__main__':
    print "Strating..."
    pool = initPool();
    
    mqttclient = mqtt.Client(client_id=mqtt_client_id)
    mqttclient.on_connect = on_connect
    mqttclient.on_message = on_message
    try:
        mqttclient.connect(mqtt_serveraddr,mqtt_serverport,60)
        mqttclient.loop_forever()
    except KeyboardInterrupt:
        pool.close();
        mqttclient.disconnect()
        
    
