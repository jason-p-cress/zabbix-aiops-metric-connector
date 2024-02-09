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
   LOG_FILENAME=logHome + "sevone-kafka-topic-reader.log"
   now = datetime.now()
   ts = now.strftime("%d/%m/%Y %H:%M:%S")
   print("opening log file " + logHome + "sevone-kafka-topic-reader.log")
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

globals().update(props)
datachannelProps = (props)
globals().update(datachannelProps)

#locals().update(props)

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
   
# Determine whether SSL communication is required for the SevOne Data Bus Kafka broker(s)

if 'sevOneKafkaSSL' in datachannelProps:
   sevOneKafkaSSL = datachannelProps['sevOneKafkaSSL'].lower()
   if sevOneKafkaSSL.lower() == "true" or sevOneKafkaSSL.lower() == "false":
      sevOneKafkaSSL = datachannelProps['sevOneKafkaSSL'].lower()
   else:
      logging.info("WARNING: Unknown 'sevOneKafkaSSL' property set. Should be \"true\" or \"false\". Defaulting to \"false\"")
      sevOneKafkaSSL = "false"
else:
   logging.info("sevOneKafkaSSL not set in props file. Defaulting to \"false\"")
   sevOneKafkaSSL = "false"

# configure SSL for SevOne Kafka, if required

if sevOneKafkaSSL == "true":
   if(os.path.exists(mediatorHome + "/conf/sevone-kafka-ssl.props")):
      sevOneKafkaSSLProps = loadProperties(mediatorHome + "/conf/sevone-kafka-ssl.props")
      logging.debug("SevOne Kafka SSL Properties: = " + json.dumps(sevOneKafkaSSLProps))
      globals().update(sevOneKafkaSSLProps)
      if(os.path.exists(sevOneSSLCaLocation)):
         logging.debug("SevOne SSL CA PEM file located at " + sevOneSSLCaLocation)
      else:
         logging.info("FATAL: SevOne Kafka SSL requested, but root CA file not found at " + sevOneSSLCaLocation)
         exit()
      if(os.path.exists(sevOneSSLCertificateLocation)):
         logging.debug("SevOne Kafka SSL server certificate file located at " + sevOneSSLCertificateLocation)
      else:
         logging.info("FATAL: SevOne Kafka SSL requested, but server certificate file not found at " + sevOneSSLCertificateLocation)
         exit()
      if(os.path.exists(sevOneSSLKeyLocation)):
         logging.debug("SevOne Kafka SSL key file located at " + sevOneSSLKeyLocation)
      else:
         logging.info("FATAL: SevONe Kafka SSL requested, but SSL key file not found at " + sevOneSSLCertificateLocation)
         exit()
   else:
      print("FATAL: SevOne Kafka SSL set to true, but SevOne Kafka SSL properties file " + mediatorHome + "/conf/sevone-kafka-ssl.props is missing.")
      logging.info("FATAL: Properties file " + mediatorHome + "/conf/sevone-kafka-ssl.props is missing.")
      exit()
else:
   logging.info("SevOne Data Bus Kafka SSL not requested")


#############################
#
# Connect to SevOne Kafka bus
#
#############################

ts = str(time.time())
print("Timestamp is : " + ts)
kafkasettings = {
    'bootstrap.servers': sevOneKafkaServers,
    'group.id': 'kafkaReaderGroup',
    'client.id': 'waiopsMetric-client-1@' + ts,
    'enable.auto.commit': False,
    'session.timeout.ms': 6000,
    'socket.timeout.ms': 3000,
    'default.topic.config': {'auto.offset.reset': 'earliest'}
}

if sevOneKafkaSSL.lower() == "true":
   kafkasslsettings = {
      'security.protocol' : 'SSL',
      'ssl.ca.location': sevOneSSLCaLocation,
      'ssl.certificate.location': sevOneSSLCertificateLocation,
      'ssl.key.location': sevOneSSLKeyLocation }
   kafkasettings.update(kafkasslsettings)
else:
   logging.debug("SevOne SDB Kafka connection does not require SSL")


logging.debug("kafka settings are: " + str(kafkasettings))

c = Consumer(kafkasettings)

logging.debug("Verifying SevOne kafka topic - listing topics")

admin_client = AdminClient(kafkasettings)
topics = admin_client.list_topics().topics
if not topics:
   logging.info("FATAL: Unable to verify connectivity and topics in SevOne kafka bus at " + sevOneKafkaServers + ". Verify kafka configuration, reconfigure, and retry.")
   print("FATAL: Unable to verify connectivity and topics in SevOne kafka bus at " + sevOneKafkaServers + ". Verify kafka configuration, reconfigure, and retry.")
   exit()
elif sevOneKafkaTopicName not in topics:
   print("FATAL: SevOne kafka topic name does not exist in SevOne kafka. Available topics: " + topics + ". Ensure proper topic configuration.")
   exit()

logging.debug("Successfully listed topics in SevOne kafka. Topics returned: " + str(topics))

logging.debug("Subscribing to SevOne kafka topic")
try:
   c.subscribe([sevOneKafkaTopicName])
except Exception as e:
   logging.info("FATAL: Unable to connect to SevOne Kafka bus at " + sevOneKafkaServers + ". Verify Kafka configuration, reconfigure, and retry.")
   print("FATAL: Unable to connect to SevOne Kafka bus at " + sevOneKafkaServers + ". Verify Kafka configuration, reconfigure, and retry.")
   exit()



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

