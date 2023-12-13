# Kafka-Client
A simple Kafka Client to try different features of IBM EventStreams.   
## Pre-req:   
* You need to have a functioning IBM Event Streams platform.    
* java 20 (java development kit needed if you want to modify the code)   

The client may work with other Kafka products, but the libraries used to build this client are ones that is needed by IBM EventStreams.   

## Guide to get started   
1) Create a folder in your local laptop and change directory to that folder.   
2) Clone the repository      
git clone https://github.com/natarajan-k/kafka-client.git   

3) You can either compile the java source file or alternatively, use the pre-built jar package.   

3.1)  Compile the source code.   

    Compile:    
    javac -classpath "lib/*:" KafkaClientV311.java   

    To run:   
    As Producer: java -classpath "lib/*:" KafkaClientV311 producer <number_of_records>  <config-file>   
    As Consumer: java -classpath "lib/*:" KafkaClientV311 consumer <config-file>   

3.2) Use the pre-built jar file.

    As Producer: java -jar pre-compiled/KafkaClient.jar producer <number_of_records> <config_file>
    As Consumer: java -jar pre-compiled/KafkaClient.jar consumer <config_file>

## config.properties file. 
The config.properties file available as part of this package is mostly self-explanatory. 
You can refer here on using the client as producer / consumer with / without schema.    
[https://ibmintegration.github.io/ibm-event-streams/basic-es.html](https://ibmintegration.github.io/ibm-event-streams/basic-es.html)   
[https://ibm-cloud-architecture.github.io/eda-tech-academy/getting-started/schema-lab/](https://ibm-cloud-architecture.github.io/eda-tech-academy/getting-started/schema-lab/)

