# Kafka-Client
A simple Kafka Client to try different features of IBM EventStreams.
Pre-req:
You need to have a functioning IBM Event Streams platform. 
java 17
The client may work with other Kafka products, but the libraries used to build this client are ones that is needed by IBM EventStreams.

Guide to get started
1) Create a folder in your local laptop and change directory to that folder.
2) Clone the repository
git clone https://github.com/natarajan-k/kafka-client.git

3) You can either compile the java source file or alternatively, use the pre-built jar package.

3.1) Compile the source code.
Compile: 
javac -classpath "lib/*:" KafkaClientV39.java
To run:
As Producer: java -classpath "lib/*:" KafkaClientV39 producer <number_of_records>  <config-file>
As Consumer: java -classpath "lib/*:" KafkaClientV39 consumer <config-file>

3,2) Use the pre-built jar file.
Usage For Producer: java -jar pre-compiled/KafkaClient.jar producer <number_of_records> <config_file>
Usage For Consumer: java -jar pre-compiled/KafkaClient.jar consumer <config_file>

