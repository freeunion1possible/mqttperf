#!/usr/bin/python

import sys
import time

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

nome=""

mqttc = mqtt.Client()
mqttc.username_pw_set(nome, nome)
mqttc.connect("10.0.1.190")
#mqttc.connect("broker.mqttdashboard.com")

mqttc.loop_start()
i=1

while True:
    print i
    temperature = i
    mqttc.publish("test-topic_mqtt", temperature)
    #mqttc.publish("sin/topic", temperature)
    i=i+1
    time.sleep(1)
    if i==1000 :
        
