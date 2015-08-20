'''
Created on Aug 19, 2015

@author: augus
'''
import sys, datetime
import MySQLdb
import paho.mqtt.client as mqtt

DEBUG = True
SAVETODB = False

mqttserveraddr = "iot.darktech.org"
mqttserverport = 1883

mqttmontopic = "$INPUT"
# mqttmontopic = "#"
# mqttmontopic = "tmp"

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
    sql = "insert into mqtt_rawmessages(topic_id, message,recvtime) value(3, '%s','%s')" % (message.payload, datetime.datetime.now())
    try:
        cur.execute(sql)
        curdb.commit()
    except Exception, e:
        print e 
        curdb.rollback()

def on_connect(client,userdata,flags,rc):
    if (DEBUG): 
        print(str(client)+" Connected with result code "+str(rc))
    client.subscribe(mqttmontopic+"/#")

def on_message(client,userdata,message):
    if (DEBUG): 
        print(message.topic+" "+str(message.payload))
    if (SAVETODB):
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
        curdb.close()
        mqttclient.disconnect()
        
    
