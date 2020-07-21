# Kafka
Projects related to Kafka


Reading AVRO Data using avro-tool

# put this in any directory you like
http://mvnrepository.com/artifact/org.apache.avro/avro-tools
Download any version and update path variable for running from anylocation

# run this from our project folder. Make sure ~/avro-tools-1.8.2.jar is your actual avro tools location
java -jar ~/avro-tools-1.8.2.jar tojson --pretty avro filename
ex : java -jar ~/avro-tools-1.8.2.jar tojson --pretty customer-specific.avro 

# getting the schema
ex: java -jar ~/avro-tools-1.8.2.jar getschema customer-specific.avro 


#Setting up local kafka clusters using docker, use below config for Windows or Mac.

version: '2'

services:
  # this is our kafka cluster.
  kafka-cluster:
    image: landoop/fast-data-dev:cp3.3.0
    environment:
      ADV_HOST: 127.0.0.1         # Change to 192.168.99.100 if using Docker Toolbox
      RUNTESTS: 0                 # Disable Running tests so the cluster starts faster
      FORWARDLOGS: 0              # Disable running 5 file source connectors that bring application logs into Kafka topics
      SAMPLEDATA: 0               # Do not create sea_vessel_position_reports, nyc_yellow_taxi_trip_data, reddit_posts topics with sample Avro records.
    ports:
      - 2181:2181                 # Zookeeper
      - 3030:3030                 # Landoop UI
      - 8081-8083:8081-8083       # REST Proxy, Schema Registry, Kafka Connect ports
      - 9581-9585:9581-9585       # JMX Ports
      - 9092:9092                 # Kafka Broker
