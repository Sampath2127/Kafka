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
