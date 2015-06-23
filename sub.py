#!/usr/bin/python

import requests
import sys
import json
import time
#libraries to logging
import logging
import tempfile


try:
    import paho.mqtt.client as mqtt
except ImportError:
    # This part is only required to run the example from within the examples
    # directory when the module itself is not installed.
    #
    # If you have the module installed, just use "import paho.mqtt.client"
    import os
    import inspect
    cmd_subfolder = os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"../src")))
    if cmd_subfolder not in sys.path:
        sys.path.insert(0, cmd_subfolder)
    import paho.mqtt.client as mqtt

    #Llamamos a la funcion de log para volcar los resultados en un fichero
    
MQTT_SERVER = "127.0.0.1"
MQTT_PORT = 1883
TOPIC_RESPONSE = "test-topic_mqtt"
lst_msg_id = []
lst_tProduc = []
lst_tConsum = []

#https://dweet.io/dweet/for/my-thing-name?hello=world&foo=bar

def on_connect(mqttc, obj, flags, rc):
    print("rc: "+str(rc))
    

class Payload(object):
    def __init__(self, j):
        self.__dict__ = json.loads(j)


def on_message(mqttc, obj, msg):
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

    value=timestamp()
    timeReceived = time.time()
    msg_size=(msg.payload).__sizeof__()
    
    if msg.topic == TOPIC_RESPONSE:
        
        #dweet with a thing name
        #print dweet_by_name(name="test_thing", data={"hello": "world"})    
        #newdata = json.loads(msg.payload)
    
        try :
            p = Payload(msg.payload)
            
            out_t1 = p.timeT1
            out_id = p.id
            out_size = p.size
        
            timeSent = out_t1
            timeElapsed = timeReceived - timeSent    
        
            print "%20s %20s %20s %20s %20s %20s %20s" % (out_id, out_t1, value, timeReceived, timeElapsed, out_size, msg_size)
            
            #store in lists
            lst_msg_id.append(out_id)
            lst_tProduc.append(out_t1)
            lst_tConsum.append(value)
            #   lst_msg_siz.append(out_size)
            
            #f.write(str(out_id)+","+str(out_t1)+","+str(value)+","+str(out_size))
            #f.write('\n')
    
            # back up every 30 seconds or
            # save the file at the end of the test
            if not out_id % 20 :
                flagend=False
                writeFile(lst_msg_id,lst_tProduc,lst_tConsum, flagend)
            
                if out_id == 10000 :
                    flagend=True
                    writeFile(lst_msg_id,lst_tProduc,lst_tConsum, flagend)
                
                try:
                    #string='https://dweet.io/dweet/for/'+TOPIC_RESPONSE+'?'+data
                    url='http://dweet.io/dweet/for/'+TOPIC_RESPONSE
                        #r = requests.get(url, params=newdata)
                        #print r.status_code
                except requests.exceptions.ConnectionError, e:
                    raise e

        except TypeError:
            print "Msg withoout JSON format"
            
    else:
        print 'Message out of topic'

    

def on_publish(mqttc, obj, mid):
    print("mid: "+str(mid))

def on_subscribe(mqttc, obj, mid, granted_qos):
    print("Subscribed: "+str(mid)+" "+str(granted_qos))

def on_log(mqttc, obj, level, string):
    print(string)


def timestamp():
   now = time.time()
   localtime = time.localtime(now)
   milliseconds = '%03d' % int((now - int(now)) * 1000)
   return time.strftime('%Y%m%d%H%M%S', localtime) + milliseconds

def timestampHuman():
   now = time.time()
   localtime = time.localtime(now)
   return time.strftime('%Y%m%d-%H%M%S', localtime)

    
def writeFile(lst_msg_id,lst_tProduc,lst_tConsum,flagend):
    
    print("saving...")
    tamano=len(lst_msg_id) 
    
    if flagend == True:
        
        nameFile = timestampHuman()+".log"
        f = open(nameFile, 'w+')
 
        for index in range(tamano):
            f.write(str(lst_msg_id[index]) + "," + str(lst_tProduc[index]) + "," + str(lst_tConsum[index]))
            f.write('\n')
    
        del lst_msg_id[:]
        del lst_tProduc[:]
        del lst_tConsum[:]
        f.flush()
        f.close()

    else :

        nameFile = timestampHuman()+".tmp"
        f = open(nameFile, 'w+')

        for index in range(tamano):
            f.write(str(lst_msg_id[index]) + "," + str(lst_tProduc[index]) + "," + str(lst_tConsum[index]))
            f.write('\n')
        f.flush()
            
            
 
# If you want to use a specific client id, use
# mqttc = mqtt.Client("client-id")
# but note that the client id must be unique on the broker. Leaving the client
# id parameter empty will generate a random id for you.
mqttc = mqtt.Client()
mqttc.on_message = on_message
mqttc.on_connect = on_connect
#mqttc.on_publish = on_publish
mqttc.on_subscribe = on_subscribe
# Uncomment to enable debug messages
#mqttc.on_log = on_log

mqttc.connect(MQTT_SERVER,MQTT_PORT,60)
#mqttc.connect("m2m.eclipse.org", 1883, 60)

mqttc.subscribe(TOPIC_RESPONSE,1)
#mqttc.subscribe("$SYS/#", 0)

mqttc.loop_forever()
