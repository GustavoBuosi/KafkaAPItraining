package com.github.simpleproducer.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
        String bootstrapServers ="127.0.0.1:9092";

    //Set producer properties:
        Properties properties= new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//                properties.setProperty("bootstrap.servers","127.0.0.1:9092");
//        properties.setProperty("key.serializer", StringSerializer.class.getName());
//        properties.setProperty("bootstrap.servers",StringSerializer.class.getName());

        //Create Producer

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //Create a Producer Record:
        ProducerRecord record = new ProducerRecord<String,String>("test_topic","hello world");


    //Send data - asynchronous, it stays on the background if there is no command to
        producer.send(record);

        //flush data
        producer.flush();
        //flush and close producer
        producer.close();
    }
}
