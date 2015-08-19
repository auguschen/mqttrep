'''
Created on Aug 19, 2015

@author: augus
'''
import MySQLdb
import paho.mqtt.client as mqtt

def initdb():
    pass

def saveTodb(db):
    pass

def on_connect(client,userdata,flags,rc):
    print("Connected with result code "+str(rc))
    client.subscribe("tmp")

def on_message(client,userdata,message):
    print(message.topic+" "+str(message.payload))


if __name__ == '__main__':
    print "Hello World"
    mqttclient = mqtt.Client()
    mqttclient.on_connect = on_connect
    mqttclient.on_message = on_message
    mqttclient.connect("iot.darktech.org",1883,60)
    mqttclient.loop_forever()
    
