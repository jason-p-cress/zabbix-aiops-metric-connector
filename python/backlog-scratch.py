#!/usr/local/bin/python

#
# SevOne Data Bus datachannel for Watson AIOps metric anomaly detection
#
# 04/01/22 - Jason Cress (jcress@us.ibm.com)
# 12/19/22 - Restructured the threading, added interrupt handler, added SSL support for SDB
#  4/20/23 - Fixed scope issues when using SSL certificates
#
# TODO:
#
# - sleep and retry when connection lost to either sdb or watson
#
#

from avro.io import DatumReader
#import avro.io
import logging
try: 
    import queue
except ImportError:
    import Queue as queue
import socket
import ssl
import time
import threading
from datetime import datetime
try:
   from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition
except ImportError:
   print("FATAL: Unable to load confluent_kafka. Make sure you have installed confluent-kafka package. It can be installed using pip as such:\n\tpip install confluent-kafka")
   exit()
from confluent_kafka.admin import AdminClient
#from confluent_kafka.schema_registry.avro import AvroDeserializer
import io
import json
import os
import sys
import re
import time
import signal

def shutdownHandler(*args):
   shutdownRequest = True
   raise SystemExit('Exiting')

def reconfigHandler(*args):
   logging.info("###############################################")
   logging.info("#                                             #")
   logging.info("# Re-reading datachannel configuration file.. #")
   logging.info("#                                             #")
   logging.info("###############################################")
   configProperties()
   
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

###############################################
#
# Function to read the metrics-ignore.conf file
#
###############################################

def loadMetricsIgnore(filepath, comment_char='#'):

   if(os.path.exists(filepath)):
      with open (filepath, "rt") as f:
         for line in f:
            l = line.strip()
            if l and not l.startswith(comment_char):
               ignoreMetrics.add(l)

###############################################
#
# Function to read the counter-metrics.conf file
#
###############################################

def loadCounters(filepath, comment_char='#'):

   if(os.path.exists(filepath)):
      with open (filepath, "rt") as f:
         for line in f:
            l = line.strip()
            if l and not l.startswith(comment_char):
               counterMetrics.add(l)

###################################
#
# Producer acknowledgement function
#
###################################

def acked(err,msg):
   if err is not None:
      logging.info("Failed to deliver message: %s: %s" % (str(msg), str(err)))
   else:
      pass
      #print("Message produced: %s" % (str(msg)))

#####################################
#
# Puts metric onto Watson kafka topic
#
#####################################

def produceMetric(metricData):

   watsonProducer.produce(watsonKafkaTopicName,key="key",value=metricData, callback=acked)
   watsonProducer.poll(0.001)

def postMetric(metricData):

   #################################################
   #
   # Not currently implemented, future functionality
   #
   #################################################

   print("Posting data to PI REST API: " + metricData)
   global restMediationServiceHost
   global restMediationServicePort
   global watsonTopicName

   #encodedMetricData = urllib.parse.urlencode(metricData)
   encodedMetricData = metricData.encode('utf-8')

   #######################################################
   #
   # Function to post metric to WAIOps for analysis
   #
   #######################################################

   method = "POST"

   requestUrl = 'http://' + restMediationServiceHost + ':' + restMediationServicePort + '/ioa/metrics'
   #requestUrl = 'http://' + restMediationServiceHost + ':' + restMediationServicePort + '/metrics/api/'
   if(watsonProductTarget == "pi"):
      requestUrl = restMediationServiceProtocol + '://' + restMediationServiceHost + ':' + restMediationServicePort + '/metrics/api/1.0/metrics'
   elif(watsonProductTarget == "aiops"):
      requestUrl = restMediationServiceProtocol + '://' + restMediationServiceHost + ':' + restMediationServicePort + '/aiops/api/app/metric-api/v1/metrics'

   #authHeader = 'Basic ' + base64.b64encode(asmServerDict["user"] + ":" + asmServerDict["password"])
   #print "auth header is: " + str(authHeader)
   #print "creating the following resource in ASM: " + jsonResource

   try:
     # request = urllib2.Request(requestUrl, metricData)
      request = urllib.request.Request(requestUrl, encodedMetricData)
      request.add_header("Content-Type",'application/json')
      request.add_header("tenantID",watsonTopicName)
      request.get_method = lambda: method

     # response = urllib2.urlopen(request)
      response = urllib.request.urlopen(request)
      xmlout = response.read()
      return True

   except IOError as e:
      print('Failed to open "%s".' % requestUrl)
      if hasattr(e, 'code'):
         print('We failed with error code - %s.' % e.code)
      elif hasattr(e, 'reason'):
         print("The error object has the following 'reason' attribute :")
         print(e.reason)
         #print("This usually means the server doesn't exist,", end=' ')
         print("is down, or we don't have an internet connection.")
      return False

def logTimeDelta(first):

   global watsonTopicAggInterval
   global longestDelta
   global intervalMetricCount
   global shutdownRequest

   ###############################################################
   #
   # This function runs every PI interval (5-minutes), and logs
   # out the following information:
   #
   #   - Average kafka latency (seconds)
   #   - Longest kafka latency (seconds)
   # 
   #   These metrics show the average/longest time it takes from
   #   SevOne's collection time to the time in which we pick it
   #   up from the SevOne databus kafka. If these numbers are 
   #   high (e.g. longer than the PI agg interval) it may be 
   #   necessary to extend PI's topic latency. Additionally, 
   #   it may be due to performance issues with the SevOne 
   #   databus kafka server.
   #
   #   - Number of metrics consumed (count)
   #
   #   The number of metrics collected during the interval. This
   #   count should be reasonably consistent every interval. 
   #   
   #   - PI Kafka producer queue length (count)
   #
   #   The number of metrics waiting to be sent to PI's Kafka
   #   topic. If this number is large (i.e. > 100) and/or
   #   continues to grow larger with every interval, then it is 
   #   likely due to a performance issue with the PI Kafka server.
   # 
   #
   ###############################################################

   while shutdownRequest != True:
      if first:
         secToInterval = (( 4 - datetime.now().minute % int(watsonTopicAggInterval)) * 60 ) + (60 - datetime.now().second)
         logging.info("Starting datachannel, " + str(secToInterval) + " seconds to next interval...")
         time.sleep(secToInterval)
      currTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
      first=False
      logging.info('==== INTERVAL STATISTICS FOR ' + currTime + '====')
      ###############################################
      #
      # If indicator/resource logging is requested...
      #
      ###############################################
      if(logUniqueIndicators.lower() == "true"):
         logging.debug('Unique indicators seen during interval: ' + str(intervalMetricSet))
      if(logUniqueResources.lower() == "true"):
         logging.debug('Unique resources seen during interval: ' + str(intervalResourceSet))
      logging.info('Longest kafka latency for interval is: ' + str(longestDelta / 1000) + ' seconds.')
      logging.info('Number of unique metric/resources consumed (metric count): ' + str(intervalMetricCount) )
      logging.info('Number of unique metric indicators: ' + str(len(intervalMetricSet)))
      logging.info('Number of unique resources: ' + str(len(intervalResourceSet)))
      logging.info('PI Kafka producer queue length: ' + str(publishQueue.qsize()))
      intervalMetricSet.clear()
      intervalResourceSet.clear()
      intervalMetricCount = 0
      longestDelta = 0
      time.sleep(int(watsonTopicAggInterval) * 60)

def logTimeDeltaCron(callback_func, first=True):

   #################################################################
   #
   # Sets up a "cron" type job that logs perf stats every 60 seconds
   #
   #################################################################
   
   if first:
      secToInterval = (( 4 - datetime.now().minute % int(watsonTopicAggInterval)) * 60 ) + (60 - datetime.now().second)
      time.sleep(secToInterval)
   callback_func()
   time.sleep(int(watsonTopicAggInterval) * 60)
   logTimeDeltaCron(callback_func, False)


def fastAvroDecode(msg_value):

   #############################################################################################
   #
   # This function decodes the SevOne avro message, and returns a PI mapped/converted dictionary
   #
   #############################################################################################

   message_bytes = io.BytesIO(msg_value)
   event_dict = fastavro.schemaless_reader(message_bytes, fastavro.parse_schema(schema))
   return event_dict


def translateToWatsonMetric(event_dict):

   ################################################################################################
   #
   # This is the translation function to translate the SevOne json format to the Watson json format
   #
   ################################################################################################

   # Build WAIOps json
   if(event_dict["indicatorName"] in ignoreMetrics):
      return("NULL")
   else:
      try:
         waiopsMetric = dict()
         waiopsMetric["attributes"] = dict()
         waiopsMetric["metrics"] = dict()
         waiopsMetric["metrics"][event_dict["indicatorName"]] = float(event_dict["value"])
         waiopsMetric["attributes"]["node"] = event_dict["deviceName"]
         waiopsMetric["attributes"]["interface"] = event_dict["objectName"]
         waiopsMetric["attributes"]["group"] = watsonMetricGroup
         if(event_dict["indicatorName"] in counterMetrics):
            waiopsMetric["attributes"]["accumulators"] = event_dict["indicatorName"]
         waiopsMetric["timestamp"] = int(event_dict["time"] * 1000)
         waiopsMetric["tenantID"] = watsonTopicName
         waiopsMetric["resourceID"] = event_dict["deviceName"] + ":" + event_dict["objectName"]
         waiopsGroup = {}
         waiopsGroup["groups"] = []
         waiopsGroup["groups"].append(waiopsMetric)
         return(waiopsGroup)
      except:
         logging.info("Unable to process message: " + json.dumps(event_dict))
         return("NULL")


def publishMetric():

   ###########################
   #
   # Publisher thread function
   #
   ###########################

   global shutdownRequest

   # block until a message hits the publish queue

   while shutdownRequest != True:
      item = publishQueue.get()
      #produceMetric(json.dumps(item))
      printMetric(json.dumps(item))
      publishQueue.task_done()


def sdbReader():

   ########################################
   #
   # SevOne Data Bus reader thread function
   #
   ########################################

    global intervalMetricCount
    global longestDelta

    shutdownRequest = False
    lastMessage = "NULL"
    try:
        while shutdownRequest != True:
            msg = c.poll(0.1)
            if msg is None:
                continue
            elif not msg.error():
               if sevOneKafkaDataFormat.lower() == "avro":
                  metricJson = translateToWatsonMetric(fastAvroDecode(msg.value()))
               else:
                  metricJson = translateToWatsonMetric(json.loads(msg.value()))
               lastMessage = metricJson
               if(metricJson == "NULL"):
                  pass
               else:
                  if 'timestamp' in metricJson["groups"][0]:
                     publishQueue.put(metricJson)
                     intervalMetricCount += 1
                     currTime = int(time.time() * 1000)
                     deltaTime = currTime - int(metricJson["groups"][0]["timestamp"])
                     if(deltaTime > longestDelta):
                        longestDelta = deltaTime
                     for key in metricJson["groups"][0]["metrics"]:
                        intervalMetricSet.add(key)
                     if 'resourceID' in metricJson["groups"][0]:
                        intervalResourceSet.add(metricJson["groups"][0]["resourceID"])
                  else:
                     logging.info("WARNING: Message received contains no timestamp field. Message is: " + json.dumps(metricJson))
                
            elif msg.error().code() == KafkaError._PARTITION_EOF:
                logging.info('End of partition reached {0}/{1}'
                      .format(msg.topic(), msg.partition()))
            else:
                logging.info('Error occured: {0}'.format(msg.error().str()))
    
    except KeyboardInterrupt:
        shutdownRequest = True
    
    finally:
       shutdownRequest = True
       now = datetime.now()
       ts = now.strftime("%d/%m/%Y %H-%M-%S")
       logging.info('Mediator shut down at: ' + ts)
       if(lastMessage):
          logging.info('Last message: ' + json.dumps(lastMessage))
       if(publishThread.is_alive()):
          
          logging.debug("publishThread is still alive")
       if(perfStatThread.is_alive()):
          logging.debug("perfStatThread is still alive")
       c.close()
       sys.stdout.close()

def setupFilePaths():
   
   ###################
   #
   # Set up file paths
   #
   ###################
   
   # The following does not work if using nuitka to compile the code into ELF binary. Using sys.argv[0] instead
   
   #mediatorBinDir = os.path.dirname(os.path.abspath(__file__))

   global mediatorHome
   global mediatorBinDir
   
   #print('sys.argv[0] =', sys.argv[0])             
   pathname = os.path.dirname(sys.argv[0])        
   #print('path =', pathname)
   mediatorBinDir = os.path.abspath(pathname) + "/" + sys.argv[0]
   
   mediatorHome = os.path.dirname(os.path.abspath(pathname))
   
   print("mediatorBinDir is: " + mediatorBinDir)
   
   print("mediatorHome is: " + mediatorHome)
   
def setupLogging():

   ################
   #
   # Set up logging
   #
   ################

   
   if(os.path.isdir(mediatorHome + "/log")):
      logHome = mediatorHome + "/log/"
   
      # Redirect stdout to log/datachannel.out

      print("Redirecting stdout to " + logHome + "datachannel.out")
      print("Redirecting stderr to " + logHome + "datachannel.err")
      #sys.stdout = open(logHome + "datachannel.out", "w")
      #sys.stderr = open(logHome + "datachannel.err", "w")
      LOG_FILENAME=logHome + "sevone-datachannelbacklog.log"
      now = datetime.now()
      ts = now.strftime("%d/%m/%Y %H:%M:%S")
      print("opening log file " + logHome + "/sevone-datachannelbacklog.log")
      try:
         if loggingLevel.upper() == "INFO" or loggingLevel.upper() == "DEBUG":
            if loggingLevel.upper() == "INFO":
               logging.basicConfig(level=logging.INFO, filename=LOG_FILENAME, filemode="w+",format="%(asctime)-15s %(levelname)-8s %(message)s")
            else:
               logging.basicConfig(level=logging.DEBUG, filename=LOG_FILENAME, filemode="w+",format="%(asctime)-15s %(levelname)-8s %(message)s")
         else:
            logging.info("WARNING: Unknown loggingLevel specified in sevone-watson-datachannel.props. Must be one of 'INFO' or 'DEBUG'. Defaulting to 'INFO'")
         logging.info("Mediator started at: " + ts + "\n")
      except:
         print("FATAL: failed to start logging. Verify logging path available and disk space.")
         exit()
   else:
      print("FATAL: unable to find log directory at " + mediatorHome + "log")
      sys.exit()

def configProperties():

   ##########################################
   #
   # Read and validate datachannel properties
   # Include 'avro' packages if required
   #
   ##########################################
   
   global ignoreMetrics
   global counterMetrics
   global sevOneKafkaServers
   global watsonKafkaServers
   global watsonTopicAggInterval
   global watsonKafkaTopicName
   global sevOneKafkaTopicName
   global logUniqueIndicators
   global logUniqueResources
   global watsonProductTarget
   global datachannelProps
   global sevOneKafkaDataFormat
   
   ignoreMetrics = set()
   loadMetricsIgnore(mediatorHome + "/conf/metrics-ignore.conf")
   counterMetrics = set()
   loadCounters(mediatorHome + "/conf/counter-metrics.conf")
   
   if(os.path.exists(mediatorHome + "/conf/sevone-watson-datachannel.props")):
      props = loadProperties(mediatorHome + "/conf/sevone-watson-datachannel.props")
   else:
      print("FATAL: Properties file " + mediatorHome + "/conf/sevone-watson-datachannel.props is missing.")
      exit()
   
   # convert properties to variables
   
   globals().update(props)
   datachannelProps = (props)
   globals().update(datachannelProps)

   
   # configure logging based on level requested

   setupLogging()

   logging.info("Metrics to be ignored: ")
   for metric in ignoreMetrics:
      logging.info(metric)
   
   logging.info("Counter/pegged metrics: ")
   for metric in counterMetrics:
      logging.info(metric)
   
   # validate required properties exist and set defaults where possible
   
   logging.debug("Locals variables:" + str(globals()))
   logging.debug("datachannelProps: " + str(datachannelProps))

   # Ensure that one or more SevOne Data Bus Kafka servers are defined

   if 'sevOneKafkaServers' not in datachannelProps:
      print("FATAL: sevOneKafkaServers not set in properties file! Specify at least one SevOne Data Bus Kafka server.")
      logging.info("FATAL: sevOneKafkaServers not set in properties file! Specify at least one SevOne Data Bus Kafka server.")
      exit()
   else:
      sevOneKafkaServers = datachannelProps['sevOneKafkaServers']
      logging.debug("sevOneKafkaServers = " + str(sevOneKafkaServers))
   
   # Ensure that one or more Watson AIOps Kafka servers are defined

   if 'watsonKafkaServers' not in datachannelProps:
      print("FATAL: watsonKafkaServers not set in properties file! Specify at least one Watson AIOps kafka server.")
      logging.info("FATAL: watsonKafkaServers not set in properties file! Specify at least one Watson AIOps kafka server.")
      exit()
   else: 
      logging.debug("Setting watsonKafkaServers to " + str(props['watsonKafkaServers']))
      watsonKafkaServers = datachannelProps['watsonKafkaServers']
      logging.debug("watsonKafkaServers: " + str(watsonKafkaServers))
      
   # Ensure that the Watson AIOps Kafka topic name has been configured

   if 'watsonKafkaTopicName' not in datachannelProps:
      print("FATAL: Watson AIOps kafka topic name not specified in properties file! Configure the topic name property \"watsonKafkaTopicName\"")
      logging.info("FATAL: Watson AIOps kafka topic name not specified in properties file! Configure the topic name property \"watsonKafkaTopicName\"")
      logging.info("The properties are: " + str(globals()))
      exit()
   else:
      logging.debug("Setting watsonKafkaTopicName to " + str(props['watsonKafkaTopicName']))
      watsonKafkaTopicName = datachannelProps['watsonKafkaTopicName']
      logging.debug("watsonKafkaTopicName = " + str(watsonKafkaTopicName))
   
   # Configure the aggregation interval, default and minimum 5

   if 'watsonTopicAggInterval' not in datachannelProps:
      watsonTopicAggInterval = "5"
      logging.info("watsonTopicAggInterval not specified in properties file. Setting to 5 minutes")
   else:
      watsonTopicAggInterval = datachannelProps['watsonTopicAggInterval']
      logging.debug("watsonTopicAggInterval = " + watsonTopicAggInterval)
   
   # Verify the SevOne Data Bus Kafka topic name

   if 'sevOneKafkaTopicName' not in datachannelProps:
      sevOneKafkaTopicName = "sdb"
      logging.info("sevOneKafkaTopicName not specified in properties file. Defaulting to \"sdb\".")
   else:
      sevOneKafkaTopicName = datachannelProps['sevOneKafkaTopicName']
   logging.debug("sevOneKafkaTopicName = " + sevOneKafkaTopicName)

   # Determine what IBM product this will integrate with, either the legacy Predictive Insights, or the Metric Anomaly Detection in either NOI or Watson AIOps AI Manager

   if 'watsonProductTarget' in datachannelProps:
      if(datachannelProps['watsonProductTarget'] == "aiops" or datachannelProps['watsonProductTarget'] == "pi"):   
         watsonProductTarget = datachannelProps['watsonProductTarget']
         logging.debug("datachannel product target is: " + watsonProductTarget)
      else:
         logging.info("FATAL: Property 'watsonProductTarget' unknown, should be \"pi\" or \"aiops\", it is configured as: " + watsonProductTarget['watsonProductTarget'])
         exit()
   else:
      logging.info("FATAL: Datachannel property 'watsonProductTarget' is not set, must be set to either \"pi\" for Predictive Insights, or \"aiops\" for NOI/WatsonAIOps")
      exit()

   # Determine whether SSL communication is required for the Watson Kafka broker(s)

   if 'watsonKafkaSSL' in datachannelProps:
      watsonKafkaSSL = datachannelProps['watsonKafkaSSL'].lower()
      if watsonKafkaSSL.lower() == "true" or watsonKafkaSSL.lower() == "false":
         watsonKafkaSSL = datachannelProps['watsonKafkaSSL'].lower()
      else:
         logging.info("WARNING: Unknown 'watsonKafkaSSL' property set. Should be \"true\" or \"false\". Defaulting to \"false\"")
         watsonKafkaSSL = "false"
   else:
      logging.info("watsonKafkaSSL not set in props file. Defaulting to \"false\"")
      watsonKafkaSSL = "false"

   # configure SSL for Watson Kafka, if required

   if watsonKafkaSSL == "true":
      if(os.path.exists(mediatorHome + "/conf/sevone-kafka-ssl.props")):
         watsonKafkaSSLProps = loadProperties(mediatorHome + "/conf/watson-kafka-ssl.props")
         logging.debug("SevOne Kafka SSL Properties: = " + json.dumps(watsonKafkaSSLProps))
         globals().update(watsonKafkaSSLProps)
         if(os.path.exists(watsonSSLCaLocation)):
            logging.debug("Watson Kafka SSL CA PEM file located at " + watsonSSLCaLocation)
         else:
            logging.info("FATAL: Watson Kafka SSL requested, but root CA file not found at " + watsonSSLCaLocation)
            exit()
         if(os.path.exists(watsonSSLCertificateLocation)):
            logging.debug("Watson Kafka SSL server certificate file located at " + watsonSSLCertificateLocation)
         else:
            logging.info("FATAL: Watson Kafka SSL requested, but server certificate file not found at " + watsonSSLCertificateLocation)
            exit()
         if(os.path.exists(watsonSSLKeyLocation)):
            logging.debug("Watson Kafka SSL key file located at " + watsonSSLKeyLocation)
         else:
            logging.info("FATAL: Watson Kafka SSL requested, but SSL key file not found at " + watsonSSLCertificateLocation)
            exit()
      else:
         print("FATAL: Watson Kafka SSL set to true, but Watson Kafka SSL properties file " + mediatorHome + "/conf/watson-kafka-ssl.props is missing.")
         logging.info("FATAL: Properties file " + mediatorHome + "/conf/watson-kafka-ssl.props is missing.")
         exit()
   else:
      logging.info("Watson Data Bus Kafka SSL not requested")

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
   
#############
#
# Begins here
#
#############

setupFilePaths()
configProperties()

global ignoreMetrics
global counterMetrics
global sevOneKafkaServers
global watsonKafkaServers
global watsonTopicAggInterval
global watstonKafkaTopicName



print("Starting datachannel")

shutdownRequest = False
intervalMetricSet = set()
intervalResourceSet = set()
longestDelta = 0
intervalNumber = 0
intervalMetricCount = 0


if 'sevOneKafkaDataFormat' in datachannelProps:
   sevOneKafkaDataFormat = datachannelProps['sevOneKafkaDataFormat'] 
   if sevOneKafkaDataFormat.lower() == "avro":
      try:
         import fastavro
      except ImportError:
         print("FATAL: Unable to load required Python package 'fastavro'. It can be installed using pip as such:\n\tpip install fastavro\n")
         exit()
      try:
         #import avro.io
         import avro.schema
      except ImportError:
         print("FATAL: Unable to load required Python package 'avro'. It can be installed using pip as such:\n\tpip install avro\n")
         exit()
   elif sevOneKafkaDataFormat.lower() != "json":
      logging.info("Unknown sevOneKafkaDataFormat value. Should be set to \"Avro\" or \"JSON\". Defaulting to \"Avro\".")
else:
   logging.info("sevOneKafkaDataFormat not specified in properties file. Setting to \"Avro\".")
   sevOneKafkaDataFormat = "Avro"
logging.debug("sevOneKafkaDataFormat = " + sevOneKafkaDataFormat)

# Configure statistical logging for the data channel

if 'logUniqueIndicators' not in datachannelProps:
   logUniqueIndicators = "false"
else:
   if datachannelProps['logUniqueIndicators'] <> "false" or datachannelProps['logUniqueIndicators'] <> "true":
      logUniqueIndicators = "false"

if 'logUniqueResources' not in datachannelProps:
   logUniqueResources = "false"
else:
   if datachannelProps['logUniqueResources'] <> "false" or datachannelProps['logUniqueResources'] <> "true":
      logUniqueResources = "false"

################################
#
# Set up avro schema if required
#
################################

if sevOneKafkaDataFormat.lower() == "avro":
   if(os.path.exists(mediatorHome + "/conf/sevone-avro-schema.json")):
      schemafile = open(mediatorHome + "/conf/sevone-avro-schema.json", "r")
      try:
         schema = json.loads(schemafile.read())
      except:
         print("FATAL: Unable to parse SevOne Avro schema file. Please verify that it is the correct format (JSON)")
         logging.info("FATAL: Unable to parse SevOne Avro schema file. Please verify that it is the correct format (JSON)")
         exit()
   else:
      print("FATAL: SevOne data format is configured for Avro, but the required Avro schema file does not exist at " + mediatorHome + "/conf/sevone-avro-schema.json")
      print("Please obtain the SevOne Avro schema in the SevOne data bus config and place the JSON in the sevone-avro-schema.json file.")
      logging.info("FATAL: SevOne data format is configured for Avro, but the required Avro schema file does not exist at " + mediatorHome + "/conf/sevone-avro-schema.json")
      logging.info("Please obtain the SevOne Avro schema in the SevOne data bus config and place the JSON in the sevone-avro-schema.json file.")
      exit()

   print("Going to parse avro schema: " + json.dumps(schema))
   try:
      parsed_schema = avro.schema.parse(json.dumps(schema))
   except Exception as e:
      logging.info("FATAL: Unable to parse Avro schema. Verify that the schema definition is correct.")
      print("FATAL: Unable to parse avro schema. Verify that the schema definition is correct.")
      print e
      exit()
   #reader = avro.io.DatumReader(parsed_schema)
   reader = DatumReader(parsed_schema)
   print("Avro schema parsed successfully")
   print("Properties loaded successfully")



###################
#
# Ignore SSL errors
#
###################

if (not os.environ.get('PYTHONHTTPSVERIFY', '') and getattr(ssl, '_create_unverified_context', None)):
      ssl._create_default_https_context = ssl._create_unverified_context

#ctx = ssl.create_default_context()
#ctx.check_hostname = False
#ctx.verify_mode = ssl.CERT_NONE

   
   

#############################
#
# Connect to Watson Kafka bus
#
#############################

logging.debug("connecting to watsonKafkaServers: " + watsonKafkaServers)

watsonKafkaConfig = {
     'bootstrap.servers': watsonKafkaServers,
     'client.id': "SevOneDatachannel@" + socket.gethostname(),
     'queue.buffering.max.ms': 5}

if watsonKafkaSSL.lower() == "true":
   kafkasslsettings = {
      'security.protocol' : 'SSL',
      'ssl.ca.location': watsonSSLCaLocation,
      'ssl.certificate.location': watsonSSLCertificateLocation,
      'ssl.key.location': watsonSSLKeyLocation }
   watsonKafkaConfig.update(kafkasslsettings)
else:
   logging.debug("Watson Kafka connection does not require SSL")

watsonProducer = Producer(watsonKafkaConfig)

watson_admin_client = AdminClient(watsonKafkaConfig)
topics = watson_admin_client.list_topics().topics
if not topics:
   logging.info("FATAL: Unable to verify connectivity and topics in the Watson kafka bus at " + watsonKafkaServers + ". Verify kafka configuration, reconfigure, and retry.")
   print("FATAL: Unable to verify connectivity and topics in the Watson kafka bus at " + watsonKafkaServers + ". Verify kafka configuration, reconfigure, and retry.")
   exit()
elif watsonKafkaTopicName not in topics:
   print("FATAL: Watson kafka topic name (" + watsonKafkaTopicName + ") does not exist in the Watson kafka. Available topics: " + topics + ". Ensure proper topic configuration.")
   exit()

logging.debug("Watson AIOps Kafka topic available.")

#############################
#
# Connect to SevOne Kafka bus
#
#############################

kafkasettings = {
    'bootstrap.servers': sevOneKafkaServers,
    'group.id': 'watson-backlog-group',
    'client.id': 'waiopsMetric-backlog-1@' + socket.gethostname(),
    'enable.auto.commit': False,
    'session.timeout.ms': 6000,
    'socket.timeout.ms': 3000,
#    'default.topic.config': {'auto.offset.reset': 'latest'}
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

print("listing partitions for sdb")

for topic in topics:
   x = admin_client.list_topics(topic=topic)
   print x.topics
   for key, value in x.topics.items():
      for keyy, valuey in value.partitions.items():
         print keyy, 'partition id: ', valuey, 'leader:', valuey.leader, 'replica: ', valuey.replicas

print("Going to read kafka topic starting on July 17")
myStartTimestamp = 1687014428000

#tp = [TopicPartition('sdb', i, myStartTimestamp) for i in range(0,8)]
#print(str(tp))





flag = True
while True:
    try:
        msg = c.poll(10)

    except SerializerError as e:
        print("Message deserialization failed for {}: {}".format(msg, e))
        break


    if msg is None:
        continue

    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            break


    print(msg.value())
    c.commit()





































exit()
myOffset = c.offsets_for_times(tp)
topicparts = [TopicPartition('sdb', i, myStartTimestamp) for i in range(0,8)]

c.seek(myOffset)
for msg in c:
   print(json.dumps(msg))

exit()
   
logging.debug("Subscribing to SevOne kafka topic")
try:
   c.subscribe([sevOneKafkaTopicName])
except Exception as e:
   logging.info("FATAL: Unable to connect to SevOne Kafka bus at " + sevOneKafkaServers + ". Verify Kafka configuration, reconfigure, and retry.")
   print("FATAL: Unable to connect to SevOne Kafka bus at " + sevOneKafkaServers + ". Verify Kafka configuration, reconfigure, and retry.")
   exit()
   
logging.debug("SevOne Kafka Topic available.")


######################
#
# Main processing loop
#
######################


# Set up signal handlers for interrupt signal (e.g. CTRL-C) and HUP signal

signal.signal(signal.SIGINT, shutdownHandler)
signal.signal(signal.SIGHUP, reconfigHandler)


# Define a publisher queue that will be used to publish metrics to Watson

publishQueue = queue.Queue()


# Start a reader thread to connect to the SevOne kafka bus and receive messages

sdbReaderThread = threading.Thread(target=sdbReader)
sdbReaderThread.daemon = True
sdbReaderThread.start()
#publishThread.join()


# Start a publisher thread that will pick up transformed metric JSON and place it on the Watson kafka topic

publishThread = threading.Thread(target=publishMetric)
publishThread.daemon = True
publishThread.start()
#publishThread.join()


# Start a performance statistics thread to keep track of various performance metrics (queue depth, etc)

perfStatThread = threading.Thread(target=logTimeDelta, args=(True,))
perfStatThread.daemon = True
perfStatThread.start()
#perfStatThread.join()


# Sleep until shutdown signal received

while threading.active_count() > 0:
    time.sleep(0.1)

