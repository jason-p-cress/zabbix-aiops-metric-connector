<h1>Watson AIOps datachannel for SevOne metrics</h1>

<h2>Summary:</h2>

   Reads data from SevOne Data Bus (SDB) kafka topic, transforms the data, and places it onto
   an alternate kafka topic for Watson AIOps metric anomaly detection ingestion.

   Supports raw JSON or Avro format.

<h2>Requirements:</h2>

   Requires librdkafka libraries to be installed. If SSL connectivity is required, you may need to
   install openssl-devel and rebuild / reinstall librdkafka to include ssl support. The process is as
   follows:

      yum install openssl-devel gcc g++
      wget https://github.com/edenhill/librdkafka/archive/refs/tags/v1.6.2.zip 
      unzip v1.6.2
      cd librdkafka-1.6.2
      ./configure
      make
      make install


   If the SevOne kafka server and/or the Watson AIOps machines aren't dns resolvable, you may have to
   place entries in the /etc/hosts file on the server where this datachannel is run:

      Failed to resolve 'sevone-kafka:9092': Name or service not known 


<h2>Installing:</h2>

   From git: 

      1. git clone https://github.ibm.com/jcress/sevone-watson-datachannel

   From zip:

      1. download zip (Code -> Download ZIP)
      2. Create a directory for the data channel to reside (e.g. /opt/IBM/scanalytics/sevone-datachannel)
      3. cd to created directory (e.g. cd /opt/IBM/scanalytics/sevone-datachannel)
      4. unzip path/to/zip/sevone-watson-datachannel.zip


<h2>Configuring:</h2>

NOTE: if SevOne is placing metrics in Avro format, you must obtain the SevOne Avro schema from the
      SevOne Data Bus server. Replace the file 'config/sevone-avro-schema.json' with the schema
      obtained from SDB.

The properties file can be found at 'config/sevone-watson-datachannel.py'. The following properties
need to be configured:

	**sevOneKafkaDataFormat** - either "Avro" or "JSON". Defaults to "Avro"
	**sevOneKafkaTopicName** - the SevOne Data Bus kafka topic name. Defaults to "sdb"
	**sevOneKafkaServers** - one or more SDB kafka server:port combinations, separated by commas. Required.

	   e.g.:
	            sevOneKafkaServers = "sevonedbhost1:9092,sevonedbhost2:9092"

	**sevOneKafkaSSL** - set to 'true' if the SevOne Data Bus Kafka server requires SSL. If true, then 
                         edit the config/sevone-kafka-ssl.props to include the CA and server certificates, 
                         and the key file.

        **watsonProductTarget** - currently not implemented
	**publishType** - currently not implemented
	**watsonKafkaServers** - one or more Watson AIOops kafka servers for the mediation service, separated
	                     by commas. Required.
	**watsonKafkaTopicName** -  the topic name of the configured kafka. Defaults to 'metrics'.
	**watsonMetricGroup** - metric group name used when subitting metrics. Defaults to 'sevone'
	**watsonTopicName** - the name of the Watson MAD topic that was created to ingest kafka/REST metrics
	                  in Predictive Insights, Metric Manager, or Watson MAD. Required.
	**watsonTopicAggInterval** - the aggregation interval of the Watson topic in minutes. Defaults to 5. Not
	                         strictly required to be set correctly, but will give accurate information
	                         regarding the number of metrics, resources, and indicators per topic 
	                         interval, which is useful for sizing.
	**watsonKafkaSSL** - set to 'true' if the Watson Kafka server requires SSL. If true, then edit the
                         config/watson-kafka-ssl.props to include the CA and server certificates, 
                         and the key file.

	**restMediationServicePort** - currently not implemented
	**logUniqueIndicators** - "true" or "false". If set to "true", every unique indicator name collected 
	                      per interval is logged in the datachannel log file. Useful to identify whether
	                      missing expected metrics are actually being received by the datachannel.
	**logUniqueResources** - "true" or "false". If set to "true", every unique resource collected per
	                     interval is logged in the datachannel log file.
	**logLevel** - For standard logging, set to "INFO". For additional/expanded logging set to "DEBUG"

If SSL communication is required for the SevOne Kafka bus, you must edit the config/sevone-kafka-ssl.props to
include the CA and server certificates, and the key file.
 

If SSL communication is required for the SevOne Kafka bus, you must edit the config/sevone-kafka-ssl.props to

<h2>Starting the datachannel:</h2>

   There are two methods of running the datachannel:
 
      --Self-contained binary--

      A binary located under the <install location>/bin directory. This binary is a 'Nuitka' binary 
      created from the included python code. It is completely self-contained and doesn't require any
      external dependencies.
       
      To start the datachannel, simply execute:

         "nohup <install location>/bin/sevone-watson-datachannel &"

      **NOTE:**
     
      If you receive the following error during runtime:

      IOError: [Errno 2] No such file or directory: '/usr/lib/python2.7/site-packages/avro/VERSION.txt'

      This is due to a current bug in Nuitka. It can be circumvented by creating the following:

         # mkdir -p /usr/lib/python2.7/site-packages/avro/
         # echo "10.1.2" >> /usr/lib/python2.7/site-packages/avro/VERSION.txt


      --Python script--

      The python code which is located under the <install location>/python directory. This code depends
      on the following python packages, which can be installed using the python package manager 'pip' as
      such:

         pip install fastavro
         pip install avro
         pip install confluent-kafka

      For general guidelines on how to install these dependencies, see the section below entitled 
      <b>"Installing python dependencies"</b>

      To start the datachannel using the python script, run:

         "nohup <install location>/python/sevone-watson-datachannel.py &"


   Note that you can expect better performance using the compiled binary code, but if you need to customize
   or make changes to the way the datachannel operates you have that freedom with the python code. Also,
   located under the 'build' directory you can find the commands used to build the nuitka binaries if you
   wish to compile your changes into a binary.


<h2>Verifying that the datachannel is working:</h2>

   The included 'pi-kafka-reader' and 'pi-kafka-reader.py' components will connect to the configured Watson
   AIOps kafka topic and print out all messages hitting the topic. This is a quick and easy way to verify 
   that data is flowing from the SevOne SDB and to the Watson AIOps topic.

   The following log files are found under the "<install location>/log" directory:

      sevone-mediator.log	- info, debug, and performance and data stats are found here
      datachannel.out		- initial startup log entries
      datachannel.err		- runtime errors (stderr)

   Performance Monitoring -

      Various performance statistics are logged to the 'sevone-mediator.log' file every Watson AIOps metric
      interval:

         Longest kafka latency for interval: This is the time it takes from SevOne collection, to the time
            that it is read off of the SevOne SDB kafka topic. This should be as close to real-time as
            possible (within seconds). If this latency time is longer than twice the Watson AIOps
            aggregation interval, you will likely need to extend the latency of the WAIOps topic. For 
            example, if the WAIOps aggregation interval is 5 minutes (300 seconds), and the kafka read
            latency is topping out at 720 seconds, you should consider running the WAIOps topic at a 
            latency of a minimum of 2 minutes.

         Number of unique metric/resources consumed: The effective metric count for the last interval (that
            is, the number of kafka recoreds collected). This information can be useful for sizing the 
            WAIOps metric manager. This number should not vary much, unless SevOne has many different
            polling frequencies (e.g. 5 minutes for some metrics, 10 minues for other metrics).

         Number of uniqe metric indicators: This is the count of unique metric names collected in the last
            interval. Should be fairly consistent per interval unless poll times vary per metric/devices.

         Number of unique resources: The number of unique resources (e.g. router:interface, etc) seen 
            during the interval. Should be reasonably consistent per interval inless poll times vary per
            devices/device groups.

         PI Kafka producer queue length: This should be as close to '0' as possible. If you are seeing a
            high number, or the number is growing with each interval, 

     Indicator and resource logging:

        You can log unique instances of resources and/or indicators seen during each aggregation interval by
        enabling the extended logging options in the sevone-datachannel.conf file:

           logUniqueIndicators = "true"
           logUniqueResources = "true"

        Depending on the size of your SevOne environment, this can cause a high amount of disk utilization.
   
<h2>Installing python dependencies:</h2>

   <h3>Note:</h3>
   This is only required if you have a need to run the python code, rather than the self-contained binary.

   These are only high-level general steps, and may differ depending on your version and distribution of
   Linux.

   For RHEL 7 and Python 2.7.x (versions supported by PI REST mediation), ensure the following yum system 
   packages are available, and install them if not:

      yum install gcc
      yum install python-devel

   If not already installed, install the Python package manager 'pip'.
   Note that on RHEL 7.x and Python 2.7.x, pip is located in the EPEL
   repository. Enabling EPEL repository and installation of pip for python 2.7.x:

      sudo yum -y install epel-release

   If the above fails, an alternate method is to install epel-release from the fedora project using wget:

      wget https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
      sudo yum -y install epel-release-latest-7.noarch.rpm

   Once the epel-release repo has been activated, install pip as such:

      sudo yum install python2-pip

   Once you have pip installed, install the following additional python setup packages:

      sudo pip install pip==20.3.4		upgrades pip
      sudo pip install setuputils
      sudo pip install -U setuputils		upgrades setuputils

   Install the python dependencies:

      sudo pip install confluent-kafka==1.5.0
      sudo pip install fastavro
      sudo pip install avro

   
   For other versions of Linux and python, the required pakages are generally the same, but the process
   may differ slightly. Consult your distribution's documentation for installing pip & setuputils, which 
   will allow you to install the three python dependencies.

