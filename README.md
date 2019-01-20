# heronpy-kafka
Approach of utilizing Kafka in Heron Spout and Bolt.

This repo provides you the minimal setup of utilizing Kafka in Heron. All the codes are written in minimal, with SSL Kafka setup. 

If you would like to learn more about the configuration of Kafka config, please refer to the documentations of librdkafka: [https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md "librdkafka-configuration") **Note: The document isn't up-to-dated!!!** 

## Library required:
1. librdkafka
	- Install this before installing `confluent_kafka` library. This part isn't easy, I suggest you to read Confluent documentations and librdkafka documentations for references.
2. confluent_kafka
	- Please follow the official `confluent_kaffa` library installation guide. 
3. heronpy
	- If you are using docker, make sure your docker image is installed with the 2 libraries above.