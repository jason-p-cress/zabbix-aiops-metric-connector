#!/usr/bin/python3.11

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

import gzip
import urllib.request, urllib.parse, urllib.error 
import base64
from avro.io import DatumReader
#import avro.io
import logging
try: 
    import queue
except ImportError:
    import queue as queue
import socket
import ssl
import time
import threading
from datetime import datetime
try:
   from confluent_kafka import Consumer, KafkaError, Producer
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


def writeZipFile(fileName, metricData):

   #myZipFile = zipfile.ZipFile(fileName + ".zip", mode='w')
   #myZipFile.writestr(fileName, metricData)
   #close(myZipFile) 

   gz = gzip.open(fileName + ".gz", 'wb')
   gz.write(metricData)
   gz.close()
   
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

def postMetric(postedData):

   # accepts a JSON payload and posts it to watson

   retryCount = 4

   if 1 == 1: 

      #global restMediationServiceHost
      #global restMediationServicePort
      #global watsonTopicName
      #global restMediationServiceAuthentication
   
      #encodedMetricData = metricData.encode('utf-8')
      encodedMetricData = postedData.encode('utf-8')
   
      #logging.debug("going to publish the following: " + str(encodedMetricData))
      #######################################################
      #
      # Function to post metric to WAIOps for analysis
      #
      #######################################################
   
      doRetry = True
      retries = 1
   
      method = "POST"
   
      logging.debug("requestURL is " + targetUrl + ", now going to post")

      while doRetry == True:
         try:
            request = urllib.request.Request(targetUrl, data=encodedMetricData)
            request.add_header("Content-Type",'application/json')
            if(watsonProductTarget == "pi"):
               request.add_header("X-TenantID",watsonTopicName)
            logging.debug("setting authentication header, if required")
            if(restMediationServiceAuthentication.lower() == "true"):
               request.add_header("Authorization",authHeader)
            if(watsonProductTarget == "aiops"):
               request.add_header("X-TenantID",watsonTenantId)
               request.add_header("Authorization", "ZenApiKey ".encode("utf-8") + zenApiKey)
            logging.debug("Posting with headers: " + str(request.headers))
            request.get_method = lambda: method
            response = urllib.request.urlopen(request)
            doRetry = False
      
         except IOError as e:
            logging.info('Failed to open "%s".' % targetUrl)
            if hasattr(e, 'code'):
               logging.info('We failed with error code - %s.' % e.code)
            if hasattr(e, 'reason'):
               logging.info("The error object has the following 'reason' attribute :")
               logging.info(e.reason)

            if retries != retryCount:
               retries = retries + 1
               logging.info("going to retry, sleeping for " + str(retries * 3) + " seconds...")
               time.sleep(retries * 3) 
            else:
               logging.info("max retries reached for metric post, saving to file and continuing...")
               savFileName = mediatorHome + "/log/metricsave__" + datetime.now().strftime("%Y-%m-%d_%H:%M:%S") + ".json"
               wrz = threading.Thread(target=writeZipFile, args=(savFileName, encodedMetricData), kwargs={})
               wrz.start()
               #savFile = open(mediatorHome + "/log/metricsave__" + currTime + ".json", "w")
               #savFile.write(encodedMetricData)
               doRetry = False 



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
      logging.info('Datachannel producer queue length: ' + str(publishQueue.qsize()))
      if(publishType.lower() == "rest"):
         logging.info('Size of metricGroup now: ' + str(len(restMetricGroup["groups"] )))
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
   if("name" not in event_dict):
      logging.info("WARNING: payload is missing indicatorName field. Will not process metric. JSON: " + json.dumps(event_dict) )
      return("NULL")
   elif(event_dict["name"] in ignoreMetrics):
      return("NULL")
   else:
      try:
         waiopsMetric = dict()
         waiopsMetric["attributes"] = dict()
         waiopsMetric["metrics"] = dict()
         if("host" in event_dict["host"]):
            waiopsMetric["attributes"]["node"] = event_dict["host"]["host"]
         else:
            logging.info("WARNING: payload is missing host field. Will not process metric. JSON: " + json.dumps(event_dict) )
         if("item_tags" in event_dict):
            foundComponentTag = 0
            foundMetricTag = 0
            component = ""
            metricname = ""
            for tag in event_dict["item_tags"]:
               logging.debug("evaluating tag: " + tag["tag"])
               if tag["tag"] == "KafkaSubcomponent":
                  component = tag["value"]
                  foundComponentTag = 1
               if tag["tag"] == "KafkaMetric":
                  metricname = tag["value"]
                  foundMetricTag = 1
            if(foundComponentTag !=1 or foundMetricTag != 1):
               logging.info("Missing either the metric tag or component found for the following metric JSON entry. Will not process metric: " + json.dumps(event_dict))
               return("NULL")
         else:
            logging.info("No 'item_tags' found in the payload. Will not process this metric. JSON: " + json.dumps(event_dict))
         if("value" in event_dict):
            waiopsMetric["metrics"][metricname] = float(event_dict["value"])
         else:
            logging.info("WARNING: payload is missing 'name' field. Will not process metric. JSON: " + json.dumps(event_dict) )
            return("NULL")
         waiopsMetric["attributes"]["component"] = component
         waiopsMetric["attributes"]["group"] = watsonMetricGroup
         if(event_dict["name"] in counterMetrics):
            waiopsMetric["attributes"]["accumulators"] = event_dict["name"]
         if("clock" in event_dict):
            waiopsMetric["timestamp"] = int(event_dict["clock"] * 1000)
         else:
            logging.info("WARNING: payload is missing 'clock' field. Will not process metric. JSON: " + json.dumps(event_dict) )
         waiopsMetric["tenantID"] = watsonTopicName
         waiopsMetric["resourceID"] = event_dict["host"]["host"] + ":" + component
         waiopsGroup = {}
         waiopsGroup["groups"] = []
         waiopsGroup["groups"].append(waiopsMetric)
         logging.debug("posting metric: " + json.dumps(waiopsGroup, indent=4))
         return(waiopsGroup)
      except Exception as error:
         logging.info("An exception occurred: " + error)
         logging.info("Unable to process message: " + json.dumps(event_dict))
         return("NULL")

def writeRawJson(rawJson):

   jsonLogFileLocation.write(json.dumps(rawJson, indent=4) + "\n") 

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
      produceMetric(json.dumps(item))
#      if(logRawJson):
#         writeRawJson(json.dumps(item))
      publishQueue.task_done()

def restQueueReader():

   global shutdownRequest
   restBatchSize = 10000
   publishFrequency = 60

   global restMetricGroup 

   restMetricGroup = {}
   restMetricGroup["groups"] = []

   # block until a message hits the publish queue

   print("Starting queue reader for REST publishing")
   lastPublishTime = int(time.time())
   while shutdownRequest != True:
      item = publishQueue.get()
      #print("got an item: " + json.dumps(item, indent=4))
      for metric in item["groups"]:
         #print("metric: " + str(metric))
         restMetricGroup["groups"].append(metric)
      if(len(restMetricGroup["groups"]) == restBatchSize):
         logging.debug("publishing batch of 10000")
         postMetric(json.dumps(restMetricGroup))
         restMetricGroup.clear()
         restMetricGroup["groups"] = []
         lastPublishTime = int(time.time())
      elif( int(time.time()) - lastPublishTime > publishFrequency  and len(restMetricGroup["groups"]) > 0 ):
         logging.debug("publishing " + str(len(restMetricGroup["groups"])) + " metrics on publishFrequency of " + str(publishFrequency))
         postMetric(json.dumps(restMetricGroup))
         restMetricGroup.clear()
         restMetricGroup["groups"] = []
         lastPublishTime = int(time.time())
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
                  print(json.dumps(metricJson))
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
                logging.info('Kafka error: end of partition reached')
                #logging.info('Kafka error: end of partition reached {0}/{1}'
                #      .format(msg.topic(), msg.partition()))
            else:
                #logging.info('Kafka error occured: {0}'.format(msg.error().str()))
                logging.info('Kafka error occured')
    
    except Exception as error:
       logging.info("An exception occurred: " + error)
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
   
   print(("mediatorBinDir is: " + mediatorBinDir))
   
   print(("mediatorHome is: " + mediatorHome))
   
def setupLogging():

   ################
   #
   # Set up logging
   #
   ################

   
   if(os.path.isdir(mediatorHome + "/log")):
      logHome = mediatorHome + "/log/"
   
      # Redirect stdout to log/datachannel.out

      print(("Redirecting stdout to " + logHome + "datachannel.out"))
      print(("Redirecting stderr to " + logHome + "datachannel.err"))
      sys.stdout = open(logHome + "datachannel.out", "w")
      sys.stderr = open(logHome + "datachannel.err", "w")
      LOG_FILENAME=logHome + "sevone-datachannel.log"
      now = datetime.now()
      ts = now.strftime("%d/%m/%Y %H:%M:%S")
      print(("opening log file " + logHome + "/sevone-datachannel.log"))
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
      print(("FATAL: unable to find log directory at " + mediatorHome + "log"))
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
   global restMediationServiceAuthentication
   global authHeader
   global targetUrl
   
   ignoreMetrics = set()
   loadMetricsIgnore(mediatorHome + "/conf/metrics-ignore.conf")
   counterMetrics = set()
   loadCounters(mediatorHome + "/conf/counter-metrics.conf")
   
   if(os.path.exists(mediatorHome + "/conf/sevone-watson-datachannel.props")):
      props = loadProperties(mediatorHome + "/conf/sevone-watson-datachannel.props")
   else:
      print(("FATAL: Properties file " + mediatorHome + "/conf/sevone-watson-datachannel.props is missing."))
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
   
   logging.debug("Global variables:" + str(globals()))
   logging.debug("datachannelProps: " + str(datachannelProps))

   # Ensure that one or more SevOne Data Bus Kafka servers are defined

   if 'sevOneKafkaServers' not in datachannelProps:
      print("FATAL: sevOneKafkaServers not set in properties file! Specify at least one SevOne Data Bus Kafka server.")
      logging.info("FATAL: sevOneKafkaServers not set in properties file! Specify at least one SevOne Data Bus Kafka server.")
      exit()
   else:
      sevOneKafkaServers = datachannelProps['sevOneKafkaServers']
      logging.debug("sevOneKafkaServers = " + str(sevOneKafkaServers))
   
   if( "publishType" in globals()):
      publishType = datachannelProps["publishType"]
      if "watsonProductTarget" in globals():
         if(watsonProductTarget.lower() == "pi" and publishType.lower() == "rest"):
            if( restMediationServiceProtocol.lower() in [ "http", "https" ] ):
               if( "restMediationServicePort" in globals() and "restMediationServiceHost" in globals()):
                  targetUrl = restMediationServiceProtocol + '://' + restMediationServiceHost + ':' + restMediationServicePort + '/metrics/api/1.0/metrics'
               else:
                  logging.info("FATAL: restMediationServicePort and/or restMediationServiceHost properties missing. Please configure these properties for PI rest mediation")
                  exit()
            else:
               logging.info("FATAL: Unknown restMediationServiceProtocol configured. Must be \'http\' or \'https\'")
               exit()
      else:
         logging.info("FATAL: watsonProductTarget not set. Must be \'aiops\' or \'pi\'")
         exit()
   else: 
      if watsonProductTarget.lower() == "pi":
         logging.info("FATAL: publishType not set in sevone-watson-datachannel.props. Please set it to \"kafka\" or \"rest\"")
         exit()
      else:
         logging.info("INFO: publishType not set but watsonProductTarget is \'aiops\', defaulting to \'rest\'")
         publishType = "rest"

   if( publishType.lower == "kafka" ):

      if watsonProductTarget.lower() == "aiops":
         logging.info("FATAL: publishType is set to \'kafka'\, but watsonProductTarget is \'aiops\'. This is an unsupported configuration. For watsonProductTarget of \'aiops\' you must use \'rest\'")
         exit()

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
            print(("FATAL: Watson Kafka SSL set to true, but Watson Kafka SSL properties file " + mediatorHome + "/conf/watson-kafka-ssl.props is missing."))
            logging.info("FATAL: Properties file " + mediatorHome + "/conf/watson-kafka-ssl.props is missing.")
            exit()
      else:
         logging.info("Watson Data Bus Kafka SSL not requested")

   elif( publishType == "rest" ):

      if 'restMediationServiceAuthentication' in datachannelProps:
         restMediationServiceAuthentication = datachannelProps['restMediationServiceAuthentication']
         logging.debug("restMediationServiceAuthentication is " + restMediationServiceAuthentication)
         if(restMediationServiceAuthentication.lower() == "true"):
            logging.debug("REST authentication required")
            if('restMediationServiceUsername' in datachannelProps and 'restMediationServicePassword' in datachannelProps):
               restMediationServiceUsername = datachannelProps['restMediationServiceUsername'].encode("utf-8")
               restMediationServicePassword = datachannelProps['restMediationServicePassword'].encode("utf-8")
               authHeader = 'Basic '.encode("utf-8") + base64.b64encode(restMediationServiceUsername + ":".encode("utf-8") + restMediationServicePassword)
               logging.debug("Auth header is: " + authHeader.decode("ascii")) 
            else:
               logging.debug("REST mediation authentication requested, but missing restMediationServiceUsername | restMediationServicePassword in the config file")
               exit()
      else:
         logging.info("Did not find restMediationServiceAuthentication property in configuration. Not requestiong authentication")
         restMediationServiceAuthentication = "false"

      

      #restMetricGroup = {}

      #restMetricGroup["groups"] = []
   
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

   # valiate required properties for a watsonProductTarget of "aiops"
   
   if watsonProductTarget == "aiops":
      if 'watsonTenantId' in datachannelProps:
         watsonTenantId = datachannelProps['watsonTenantId']
         logging.debug("AIOps tenant id is: " + watsonTenantId)
         if(watsonTenantId != "cfd95b7e-3bc7-4006-a4a8-a73a79c71255"):
            logging.info("WARNING: Watson/AIOps TenantId is not the default TenantId")
      else:
         logging.info("NOTE: datachannel property \'watsonTenantId\' is missing in the configuration file. Using default")
         watsonTenantId = "cfd95b7e-3bc7-4006-a4a8-a73a79c71255"
      
      if 'watsonRestRoute' in datachannelProps:
         watsonRestRoute = datachannelProps['watsonRestRoute'] 
         targetUrl = watsonRestRoute
      else:
         logging.info("FATAL: watsonRestRoute not configured in datachannel properties file. Add the property should be in the form: https://myOpenshiftDNSName/aiops/api/app/metric-api/v1/metrics")
         exit()

      if 'watsonApiKey' in datachannelProps:
         watsonApiKey = datachannelProps['watsonApiKey']
      else:
         logging.info("FATAL: watsonApiKey not configured in datachannel properties file. Create an API Key and configure the watsonApiKey property")
         exit()

      if 'watsonUser' in datachannelProps:
         watsonUser = datachannelProps['watsonUser']
      else:
         logging.info("FATAL: watsonUser is not configured in datachannel properties file. Create an API Key and configure the watsonUser property")
         exit()
      
      global zenApiKey

      zenApiKey = base64.b64encode(watsonUser.encode("utf-8") + ":".encode("utf-8") + watsonApiKey.encode("utf-8"))

#   # Determine whether SSL communication is required for the Watson Kafka broker(s)
#
#   if 'watsonKafkaSSL' in datachannelProps:
#      watsonKafkaSSL = datachannelProps['watsonKafkaSSL'].lower()
#      if watsonKafkaSSL.lower() == "true" or watsonKafkaSSL.lower() == "false":
#         watsonKafkaSSL = datachannelProps['watsonKafkaSSL'].lower()
#      else:
#         logging.info("WARNING: Unknown 'watsonKafkaSSL' property set. Should be \"true\" or \"false\". Defaulting to \"false\"")
#         watsonKafkaSSL = "false"
#   else:
#      logging.info("watsonKafkaSSL not set in props file. Defaulting to \"false\"")
#      watsonKafkaSSL = "false"
#
#   # configure SSL for Watson Kafka, if required
#
#   if watsonKafkaSSL == "true":
#      if(os.path.exists(mediatorHome + "/conf/sevone-kafka-ssl.props")):
#         watsonKafkaSSLProps = loadProperties(mediatorHome + "/conf/watson-kafka-ssl.props")
#         logging.debug("SevOne Kafka SSL Properties: = " + json.dumps(watsonKafkaSSLProps))
#         globals().update(watsonKafkaSSLProps)
#         if(os.path.exists(watsonSSLCaLocation)):
#            logging.debug("Watson Kafka SSL CA PEM file located at " + watsonSSLCaLocation)
#         else:
#            logging.info("FATAL: Watson Kafka SSL requested, but root CA file not found at " + watsonSSLCaLocation)
#            exit()
#         if(os.path.exists(watsonSSLCertificateLocation)):
#            logging.debug("Watson Kafka SSL server certificate file located at " + watsonSSLCertificateLocation)
#         else:
#            logging.info("FATAL: Watson Kafka SSL requested, but server certificate file not found at " + watsonSSLCertificateLocation)
#            exit()
#         if(os.path.exists(watsonSSLKeyLocation)):
#            logging.debug("Watson Kafka SSL key file located at " + watsonSSLKeyLocation)
#         else:
#            logging.info("FATAL: Watson Kafka SSL requested, but SSL key file not found at " + watsonSSLCertificateLocation)
#            exit()
#      else:
#         print("FATAL: Watson Kafka SSL set to true, but Watson Kafka SSL properties file " + mediatorHome + "/conf/watson-kafka-ssl.props is missing.")
#         logging.info("FATAL: Properties file " + mediatorHome + "/conf/watson-kafka-ssl.props is missing.")
#         exit()
#   else:
#      logging.info("Watson Data Bus Kafka SSL not requested")

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
         print(("FATAL: SevOne Kafka SSL set to true, but SevOne Kafka SSL properties file " + mediatorHome + "/conf/sevone-kafka-ssl.props is missing."))
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
global targetUrl


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
   if datachannelProps['logUniqueIndicators'] != "false" or datachannelProps['logUniqueIndicators'] != "true":
      logUniqueIndicators = "false"

if 'logUniqueResources' not in datachannelProps:
   logUniqueResources = "false"
else:
   if datachannelProps['logUniqueResources'] != "false" or datachannelProps['logUniqueResources'] != "true":
      logUniqueResources = "false"

if 'logRawJson' not in datachannelProps:
   if datachannelProps['logRawJson'] != "false" or datachannelProps['logRawJson'] != "true":
      logRawJson = "false"
else:
   if(logRawJson.lower() == "true"):
      jsonLogFileLocation = open(mediatorHome + "/log/rawJson.log", "w")
      jsonLogFileLocation.write("Starting logging")
   
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
      print(("FATAL: SevOne data format is configured for Avro, but the required Avro schema file does not exist at " + mediatorHome + "/conf/sevone-avro-schema.json"))
      print("Please obtain the SevOne Avro schema in the SevOne data bus config and place the JSON in the sevone-avro-schema.json file.")
      logging.info("FATAL: SevOne data format is configured for Avro, but the required Avro schema file does not exist at " + mediatorHome + "/conf/sevone-avro-schema.json")
      logging.info("Please obtain the SevOne Avro schema in the SevOne data bus config and place the JSON in the sevone-avro-schema.json file.")
      exit()

   print(("Going to parse avro schema: " + json.dumps(schema)))
   try:
      parsed_schema = avro.schema.parse(json.dumps(schema))
   except Exception as e:
      logging.info("FATAL: Unable to parse Avro schema. Verify that the schema definition is correct.")
      print("FATAL: Unable to parse avro schema. Verify that the schema definition is correct.")
      print(e)
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

   
# Define a publisher queue that will be used to publish metrics to Watson

publishQueue = queue.Queue()
   

####################################################
#
# Connect to Watson Kafka bus if publishing to kafka
#
####################################################

logging.debug("Validate publisher type and if Kafka, configure Kafka properties, and if REST, start a restQueueThread")

if( "publishType" not in globals()):

   logging.info("FATAL: publishType not set in sevone-watson-datachannel.props. Please set it to \"kafka\" or \"rest\"")
   exit()

if publishType.lower() == "kafka":

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
      print(("FATAL: Unable to verify connectivity and topics in the Watson kafka bus at " + watsonKafkaServers + ". Verify kafka configuration, reconfigure, and retry."))
      exit()
   elif watsonKafkaTopicName not in topics:
      print(("FATAL: Watson kafka topic name (" + watsonKafkaTopicName + ") does not exist in the Watson kafka. Available topics: " + topics + ". Ensure proper topic configuration."))
      exit()
   
   logging.debug("Watson AIOps Kafka topic available.")



elif publishType.lower() == "rest":

   # start up a thread to pick up the queue messages and add them to the restMetricGroup["groups"] 
   logging.debug("publishType is \'rest\', let's start a restQueueThread")
   
   restQueueThread = threading.Thread(target=restQueueReader)
   restQueueThread.daemon = True
   restQueueThread.start()
   # start up a thread to publish every 60 seconds

#   restPublishThread = threading.Thread(target=postMetric)
#   restPublishThread.daemon = True
#   restPublishThread.start()
   #perfStatThread.join()

   

else:

   logging.info("FATAL: unknown publishType property. Should be \"kafka\" or \"rest\". Please check the properties file and ensure publishType is configured properly.")
   exit()


#############################
#
# Connect to SevOne Kafka bus
#
#############################

kafkasettings = {
    'bootstrap.servers': sevOneKafkaServers,
    'group.id': 'waiopsMetric-Restgroup',
    'client.id': 'waiopsMetric-Restclient-1@' + socket.gethostname(),
    'enable.auto.commit': False,
    'session.timeout.ms': 6000,
    'socket.timeout.ms': 3000,
    'default.topic.config': {'auto.offset.reset': 'latest'}
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

c = Consumer(kafkasettings)    # is this version buggy? thinks this is a Producer



logging.debug("Verifying SevOne kafka topic - listing topics")
logging.info("==============================================")
logging.info("= IGNORE FOLLOWING PRODUCER WARNINGS         =")
logging.info("==============================================")


admin_client = AdminClient(kafkasettings)
topics = admin_client.list_topics().topics
if not topics:
   logging.info("FATAL: Unable to verify connectivity and topics in SevOne kafka bus at " + sevOneKafkaServers + ". Verify kafka configuration, reconfigure, and retry.")
   print(("FATAL: Unable to verify connectivity and topics in SevOne kafka bus at " + sevOneKafkaServers + ". Verify kafka configuration, reconfigure, and retry."))
   exit()
elif sevOneKafkaTopicName not in topics:
   print(("FATAL: SevOne kafka topic name does not exist in SevOne kafka. Available topics: " + topics + ". Ensure proper topic configuration."))
   exit()

logging.debug("Successfully listed topics in SevOne kafka. Topics returned: " + str(topics))

logging.debug("Subscribing to SevOne kafka topic")
try:
   c.subscribe([sevOneKafkaTopicName])
except Exception as e:
   logging.info("FATAL: Unable to connect to SevOne Kafka bus at " + sevOneKafkaServers + ". Verify Kafka configuration, reconfigure, and retry.")
   print(("FATAL: Unable to connect to SevOne Kafka bus at " + sevOneKafkaServers + ". Verify Kafka configuration, reconfigure, and retry."))
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




# Start a reader thread to connect to the SevOne kafka bus and receive messages

sdbReaderThread = threading.Thread(target=sdbReader)
sdbReaderThread.daemon = True
sdbReaderThread.start()
#publishThread.join()


# Start a publisher thread that will pick up transformed metric JSON and place it on the Watson kafka topic

if(publishType.lower == "kafka"):
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

