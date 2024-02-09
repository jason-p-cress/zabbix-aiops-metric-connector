#!/usr/local/bin/python

#
# Prints records from the Watson AIOps / Predictive Insights kafka topic
#
#
# 04/01/22 - Jason Cress (jcress@us.ibm.com)
#


import sys
import logging
import socket
import ssl
import time
from datetime import datetime
try:
   from confluent_kafka import Consumer, KafkaError, Producer
except ImportError:
   print("FATAL: Unable to load confluent_kafka. Make sure you have installed confluent-kafka package. It can be installed using pip as such:\n\tpip install confluent-kafka")
   exit()
from confluent_kafka.admin import AdminClient
import io
import json
import os
import re
import time


######################################################
#
# Function to load and validate datachannel properties
#
######################################################

def loadProperties(filepath, sep='=', comment_char='#'):

    # Read the file passed as parameter as a properties file.
    
    props = {}
    with open(filepath, "rt") as f:
        for line in f:
            l = line.strip()
            if l and not l.startswith(comment_char) and "=" in l:
                key_value = l.split(sep)
                key = key_value[0].strip()
                value = sep.join(key_value[1:]).strip().strip('"')
                props[key] = value

    return props



#############
#
# Begins here
#
#############

intervalNumber = 0
intervalMetricCount = 0

###############################
#
# Set up file paths and logging
#
###############################


# The following does not work if using nuitka to compile the code into ELF binary. Using sys.argv[0] instead

#mediatorBinDir = os.path.dirname(os.path.abspath(__file__))


print('sys.argv[0] =', sys.argv[0])
pathname = os.path.dirname(sys.argv[0])
print('path =', pathname)
mediatorBinDir = os.path.abspath(pathname) + "/" + sys.argv[0]

mediatorHome = os.path.dirname(os.path.abspath(pathname))

print("mediatorBinDir is: " + mediatorBinDir)

print("mediatorHome is: " + mediatorHome)


if(os.path.isdir(mediatorHome + "/log")):
   logHome = mediatorHome + "/log/"
   LOG_FILENAME=logHome + "sevone-mediator.log"
   now = datetime.now()
   ts = now.strftime("%d/%m/%Y %H:%M:%S")
   print("opening log file " + logHome + "pi-kafka-topic-reader.log")
   try:
      logging.basicConfig(level=logging.DEBUG, filename=LOG_FILENAME, filemode="w+",format="%(asctime)-15s %(levelname)-8s %(message)s")
      logging.info("Mediator started at: " + ts + "\n")
   except Exception as e:
      print("FATAL: failed to start logging. Verify logging path available and disk space.")
      print("error: " + e.message)
      sys.exit()
else:
   logging.info("FATAL: unable to find log directory at " + mediatorHome + "log")
   print("FATAL: unable to find log directory at " + mediatorHome + "log")
   sys.exit()




##########################################
#
# Read and validate datachannel properties
# Include 'avro' packages if required
#
##########################################

if(os.path.exists(mediatorHome + "/conf/sevone-watson-datachannel.props")):
   props = loadProperties(mediatorHome + "/conf/sevone-watson-datachannel.props")
   print("Properties: = " + json.dumps(props))
   print("Properties: = " + json.dumps(props))
else:
   print("FATAL: Properties file " + mediatorHome + "/conf/sevone-watson-datachannel.props is missing.")
   print("FATAL: Properties file " + mediatorHome + "/conf/sevone-watson-datachannel.props is missing.")
   exit()

# convert properties to variables

locals().update(props)

# validate required properties exist and set defaults where possible

if 'watsonKafkaServers' not in locals():
   print("FATAL: watsonKafkaServers not set in properties file! Specify at least one Watson AIOps kafka server.")
   print("FATAL: watsonKafkaServers not set in properties file! Specify at least one Watson AIOps kafka server.")
   exit()
print("watsonKafkaServer(s): " + watsonKafkaServers)
   
if 'watsonKafkaTopicName' not in locals():
   print("FATAL: Watson AIOps kafka topic name not specified in properties file! Configure the topc name property \"watsonKafkaTopicName\"")
   print("FATAL: Watson AIOps kafka topic name not specified in properties file! Configure the topc name property \"watsonKafkaTopicName\"")
   exit()
print("watsonKafkaTopicName = " + watsonKafkaTopicName)

if 'watsonTopicAggInterval' not in locals():
   watsonTopicAggInterval = "5"
   print("watsonTopicAggInterval not specified in properties file. Setting to 5 minutes")
print("watsonTopicAggInterval = " + watsonTopicAggInterval)

print("Properties loaded successfully")
   
#############################
#
# Connect to Watson Kafka bus
#
#############################

kafkasettings = {
    'bootstrap.servers': watsonKafkaServers,
    'group.id': 'waiopsMetric-group',
    'client.id': 'waiopsMetric-client-1@' + socket.gethostname(),
    'enable.auto.commit': False,
    'session.timeout.ms': 6000,
    'socket.timeout.ms': 3000,
    'default.topic.config': {'auto.offset.reset': 'earliest'}
}

c = Consumer(kafkasettings)

try:
   c.subscribe([watsonKafkaTopicName])
except Exception as e:
   print("FATAL: Unable to connect to Watson Kafka bus at " + watsonKafkaServers + ". Verify Kafka configuration, reconfigure, and retry.")
   print("FATAL: Unable to connect to SevOne Kafka bus at " + watsonKafkaServers + ". Verify Kafka configuration, reconfigure, and retry.")
   print("Error: " + e.message)
   sys.exit()
   
print("Verifying Watson Kafka topic...")
try:
   topics = c.list_topics(watsonKafkaTopicName,10)
except Exception as e:
   print("FATAL: Unable to connect to Kafka bus at " + watsonKafkaServers + ". Verify Kafka configuration, reconfigure, and retry.")
   print("FATAL: Unable to connect to Kafka bus at " + watsonKafkaServers +  ". Verify Kafka configuration, reconfigure, and retry.")
   print("Error: " + e.message)
   sys.exit()

if topics.topics.get(watsonKafkaTopicName) is None:
   print("FATAL: Configured Watson topic (" + watsonKafkaTopicName + ") does not exist in Kafka. Verify Kafka configuration, reconfigure, and retry.")
   print("FATAL: Configured SevOne topic (" + watsonKafkaTopicName + ") does not exist in Kafka. Verify Kafka configuration, reconfigure, and retry.")
   sys.exit()
print("Watson Kafka Topic available.")


######################
#
# Main processing loop
#
######################

try:
    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():
           msg_value = msg.value()
           print(msg_value)
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            print('End of partition reached {0}/{1}'
                  .format(msg.topic(), msg.partition()))
        else:
            print('Error occured: {0}'.format(msg.error().str()))

except KeyboardInterrupt:
    pass

finally:
   now = datetime.now()
   ts = now.strftime("%d/%m/%Y %H-%M-%S")
   print('Reader shut down at: ' + ts)
   c.close()

