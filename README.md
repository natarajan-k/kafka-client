# Kafka - Client - Java
A simple Kafka Client in Java.   
Supports producing / consuming messages. 
Supports Schema Registry access.   
Tested with IBM Event Streams and Confluent. 

## Pre-Req:

* You need to have a functioning Confluent platform. 
* Java version at least 25. 

## Limitation
* mTLS access to Schema Registry is not fully supported. 

## Main Changes
### Version 4.00
1) Migrated to use Confluent libraries. 
2) Removed support for Apicurio Schema Registry.   

### Version 3.40
1) Added a delay between flushing messages and closing producer - to provide time for producer to push metrics to brokers (KIP-714 Metrics).   
2) Included seperate client.id for Producer and consumer. Previously client-id for producers were auto-assigned by Kafka.
3) Included Apicurio Schema GlobalId to be pushed in header to be compatible with the older client version.  

### Version 3.30
1. Added Support for Confluent Schema Registry 

## Guide to Getting Started
1. Create a folder in your local laptop and change directory to that folder. 
2. Clone the repository.   
git clone https://github.com/natarajan-k/kafka-client.git.     
Alternatively, download the [zip file](https://ibm.biz/kafka-client).   

3. Update the properties file. 

4. Test sending / receiving messages.  

        As Producer:   
        java -jar KafkaClient.jar producer  <number_of_records>  <config-file> 
        As Consumer:
        java -jar KafkaClient.jar consumer <config-file> 
        To send a custom JSON message:
        java -jar KafkaClient.jar custom <config-file> {"first_name": "Rajan","last_name": "K","country": "Malaysia","age": 56}'

## Config File
The config.properties file available as part of this package is mostly self-explanatory. 
