package com.github.nocompany.kafka.simpleproducer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-consumer-test-application"; //Note that after we execute this topic, without
        //a reset offsets option
        //on our second run we would not be consuming anything! We can also change the group Id to reset the application.
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //In consumer we need to deserialize.
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //This always reset the offset for the group.

       //Create Consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        //Subscribe to a topic:
        //consumer.subscribe(Collections.singleton("test_topic")): this configuration will guarantee that you are only fetching data from one topic.
        consumer.subscribe(Arrays.asList("test_topic"));
        //poll for new data:
        while(true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100)); //After Kafka 2.0.0 the
            // argument must be of type Duration. Using this
            //requires a maven import to force usage of Java 8.
            for (ConsumerRecord<String,String> record: records){
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            }
        }
    }
}
