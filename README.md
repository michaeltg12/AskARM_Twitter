# Multi-dependency ARM Social Outreach

This project leverages several 3rd party python packages to live monitor twitter streams, produce messages to Kafka, 
consume messages from Kafka, apply part of speech (PoS) tagging, search ARM solr instance for descriptive keyword, 
return a list of datastreams ranked by score and tweet back to the origin of the incomming tweet with relevant links to
Data Discovery.

## Getting started 

Dependencies for this project are listed in requirements.txt in the project directory. The main 3rd party dependencies 
used are kafka, tweepy, nltk, and pysolr. This project also requires the user to define some variables in the config.py 
file which should be in the main project directory. The user will need to have a key and token for the twitter api which
can be obtained from https://apps.twitter.com/. The variables that will need to be defined for the twitter api are 
CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET. The user will also have to define the solr endpoints, 
the default endpoint is browser_0, ds_info and db_table will be imported by solr_search as well. This application also 
requires there to be a running kafka server. The kafka server requires a zookeeper configuration manager to be running 
first, then the kafka server. The kafka server needs to have a topic called "tweets". Once this is running the python 
scripts for kafka producer and consumer should be started. 

## Downloading Kafka
The easiest way to install Kafka is to download binaries and run it. Since it’s based on JVM languages like Scala and 
Java, you must make sure that you are using Java 7 or greater.

Kafka is available in two different flavors: One by [Apache foundation](https://kafka.apache.org/downloads) and other by
[Confluent](https://www.confluent.io/about/) as a [package](https://www.confluent.io/download/). For this project I used
the one provided by Apache foundation.

## Starting Zookeeper
Kafka relies on Zookeeper, in order to make it run we will have to run Zookeeper first.
```
kafka-1.1.0-src/bin/zookeeper-server-start.sh config/zookeeper.properties
```
it will display lots of text on the screen, if see the following it means it’s up properly.
```
2018-06-10 06:36:15,023] INFO maxSessionTimeout set to -1 (org.apache.zookeeper.server.ZooKeeperServer)
[2018-06-10 06:36:15,044] INFO binding to port 0.0.0.0/0.0.0.0:2181 (org.apache.zookeeper.server.NIOServerCnxnFactory)
```

## Starting the Kafka Server
Next, start Kafka broker server:
```
bin/kafka-server-start.sh config/server.properties
```
And if you see the following text on the console it means it’s up.
```
2018-06-10 06:38:44,477] INFO Kafka commitId : fdcf75ea326b8e07 (org.apache.kafka.common.utils.AppInfoParser)
[2018-06-10 06:38:44,478] INFO [KafkaServer id=0] started (kafka.server.KafkaServer)
```

## Create Topic
Messages are published in topics. Use this command to create a new topic.
```
kafka-1.1.0-src/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic tweets 
```
You can also list all available topics by running the following command.
```
kafka-1.1.0-src/bin/kafka-topics.sh --list --zookeeper localhost:2181
```

## Setting up Python env
First verify that python 3 and pip are available. Then install all requiremnts using the following command.
```
pip install -r requirements.txt
```
## Running python scripts
Running the python scripts using the commands: 
```
python3.7 kafka_twitter_producer.py &
python3.7 kafka_twitter_consumer.py &
```
Or with the following bash script:
```
#!/bin/bash

python3.7 kafka_twitter_producer.py 2 &
producer_pid=$!
echo 'started producer with pid '$producer_pid
python3.7 kafka_twitter_consumer.py 2 &
consumer_pid=$!
echo 'started consumer with pid '$consumer_pid
while :
do
        if ! ps -p $producer_pid > /dev/null; then
                echo 'restarting producer'
                python3.7 kafka_twitter_producer.py &
                producer_pid=$!
        fi
        if ! ps -p $consumer_pid > /dev/null; then
                echo 'restarting consumer'
                python3.7 kafka_twitter_consumer.py &
                consumer_pid=$!
        fi
        echo 'still running' `date`
        sleep 2m
done
```

## Version Tracking
Version tracking done through [gitLab repository](https://code-int.ornl.gov/ofg/AskARM_Twitter)

## Authors
* **Michael Giansiracusa** - *Initial work* - giansiracumt@ornl.gov

## Acknowledgments
* **Ranjeet Devarakonda** and **Jitu Kumar** - For outstanding support and patience