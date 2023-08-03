import json
import traceback
import time
import paho.mqtt.client as mqtt
from paho.mqtt.client import MQTTv311, MQTTv31
import sys
from datetime import datetime
import threading

# See this for more details
# https://test.mosquitto.org/

CONFIG = json.load(open(sys.argv[1], "r"))

client = mqtt.Client(
    client_id='', 
    clean_session=True, 
    userdata=None, 
    protocol=MQTTv311
)

def on_connect(client, userdata, flags, rc):
    print("on_connect()")
    try:
        for sub_topic in CONFIG["sub_topics"]:
            client.subscribe(sub_topic)
    except:
        print("subscribe exception:\n%s" % traceback.format_exc())
            
def on_message(client, userdata, msg):
    print("msg.payload: %s" % str(msg.payload))

client.on_connect = on_connect
client.on_message = on_message

def timer_publisher_thr():
    while True:
        now = datetime.now() # current date and time
        try:
            for pub_topic in CONFIG["pub_topics"]:
                payload = "%s -> %s" % (pub_topic, 
                                        now.strftime("%m/%d/%Y, %H:%M:%S"))
                client.publish(pub_topic, payload)
        except:
            print("publish exception:\n%s" % traceback.format_exc())
        time.sleep(4)

PublishTimerThr = threading.Thread(target=timer_publisher_thr)

while True:
    print('start()')
    while True:
        try:
            client.connect(CONFIG["broker"], CONFIG["port"], 60)
            break
        except:
            print("connection failed, try again...")
            time.sleep(5)

    PublishTimerThr.start()
    client.loop_forever()

# --------------------------------------------------------------------------- #
# end of file
# --------------------------------------------------------------------------- #