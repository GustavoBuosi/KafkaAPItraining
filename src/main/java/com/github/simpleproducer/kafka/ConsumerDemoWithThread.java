package com.github.simpleproducer.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-consumer-test-application"; //Note that after we execute this topic, without
        String topic = "test_topic";
        //a reset offsets option on our second run we would not be consuming anything!
        // We can also change the group Id to reset the application.
Runnable myConsumerThread = new ConsumerThread(
        bootstrapServers,
        topic,
        groupId
);
    }
    public class ConsumerThread implements Runnable {
//Implements Runnable requires the following Override on Run:

        private CountDownLatch latch;
        private KafkaConsumer<String,String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());


        public ConsumerThread(CountDownLatch latch, String bootstrapServers, String topic, String groupId) {
            this.latch = latch;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            //In consumer we need to deserialize.
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //This always reset the offset for the group.
        }
            @Override
        public void run() {
            //poll for new data
                try{
                while(true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); //After Kafka 2.0.0 the
                    // argument must be of type Duration. Using this
                    //requires a maven import to force usage of Java 8.
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
                } catch (WakeupException e) {//
                    logger.info("Received shutdown signal!");
                } finally {
                consumer.close();
                // tell our main code we're done with the consumer.
                latch.countDown();
                }
        }
            public void shutdown(){
            //wakeup() method is used to interrupt consumer.poll()
        consumer.wakeup();
        }
        }
    }

