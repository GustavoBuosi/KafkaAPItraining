package com.github.nocompany.kafka.simpleproducer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";
        String topic = "test_topic";
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //In consumer we need to deserialize.
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //This always reset the offset for the group.

       //Create Consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        //Subscribe to a topic:
        //consumer.subscribe(Collections.singleton("test_topic")): this configuration will guarantee that you are only fetching data from one topic.
        consumer.subscribe(Arrays.asList("test_topic"));

        //assign and seek are mostly used to replay data or fetch a specific message
        //you are only reading from a single partition here.

        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToReadFrom = 15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //seek: read only an offset from a single partition

        consumer.seek(partitionToReadFrom,offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesSoFar = 0;

        //poll for new data:
        while(true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100)); //After Kafka 2.0.0 the
            // argument must be of type Duration. Using this
            //requires a maven import to force usage of Java 8.
            for (ConsumerRecord<String,String> record: records){
                numberOfMessagesSoFar = numberOfMessagesSoFar + 1;
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                if (numberOfMessagesSoFar >= numberOfMessagesToRead){
                    keepOnReading = false; //exit while loop
                    break; //exit for loop
                }
            }

        }
    }
}
