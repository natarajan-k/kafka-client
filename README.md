# Kafka - Client - Java
A simple Kafka Client in Java.   
Supports producing / consuming messages. 
Supports Schema Registry access.   
Tested with IBM Event Streams and Confluent. 

## Pre-Req:

* You need to have a functioning IBM Event Streams or Confluent platform. 
* Java version at least 25. 

## Limitation
* mTLS access to Schema Registry is not fully supported. 

## Main Changes

### Version 3.30
1. Added Support for Confluent Schema Registry 

## Guide to Getting Started
1. Create a folder in your local laptop and change directory to that folder. 
2. Clone the repository.   
git clone https://github.com/natarajan-k/kafka-client.git.     
Alternatively, download the zip file inside the pre-compiled folder and unzip it locally.   
3. Update the properties file. 

4. Test sending / receiving messages.  

        As Producer:   
        java -jar KafkaClient.jar producer  <number_of_records>  <config-file> 
        As Consumer:
        java -jar KafkaClient.jar consumer <config-file> 

## Config File
The config.properties file available as part of this package is mostly self-explanatory. 
