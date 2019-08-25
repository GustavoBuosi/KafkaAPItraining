package com.github.simpleproducer.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
       final  Logger logger = LoggerFactory.getLogger(ProducerDemo.class);
        String bootstrapServers ="127.0.0.1:9092";

    //Set producer properties:
        Properties properties= new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
//        properties.setProperty("key.serializer", StringSerializer.class.getName());
//        properties.setProperty("bootstrap.servers",StringSerializer.class.getName());

        //Create Producer

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i=0; i<10; i++) {
            //Create a Producer Record:
            String key = "id_" + Integer.toString(i);
            ProducerRecord record = new ProducerRecord<String, String>("test_topic", "hello world" + Integer.toString(i));


            //Send data - asynchronous, it stays on the background if there is no command to
            producer.send(record, new Callback() {
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            //Function to support logging of metadata when a deliver is successful or to throw an exception when it
                            //is not.
                            if (e == null) {
                                logger.info("Received new metadata. \n" +
                                        "Topic:" + recordMetadata.topic() + "\n" +
                                        "Partition: " + recordMetadata.partition() + "\n" +
                                        "Offset: " + recordMetadata.offset() + "\n" +
                                        "Timestamp: " + recordMetadata.timestamp()
                                );
                            } else {
                                logger.error("Error while producing to topic: ", e);
                            }
                        }
                    }
            );
        }
        //flush data
        producer.flush();
        //flush and close producer
        producer.close();
    }
}
