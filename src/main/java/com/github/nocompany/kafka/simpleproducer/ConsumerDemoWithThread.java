package com.github.nocompany.kafka.simpleproducer;

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
        new ConsumerDemoWithThread().run();
    }

    private ConsumerDemoWithThread(){

    }

    public void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-consumer-test-application-second"; //Note that after we execute this topic, without
        String topic = "test_topic";
        //a reset offsets option on our second run we would not be consuming anything!
        // We can also change the group Id to reset the application.

       //Latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        logger.info("Creating the consumer thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(
                latch,
                bootstrapServers,
                topic,
                groupId
        );
        //Starting consumer thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
        logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        ));

        try {
            latch.await();
        }
        catch (InterruptedException e){
            logger.error("Application got interrupted", e);
        }
        finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {
//Implements Runnable requires the following Override on Run:

        //Variable declaration:
        private CountDownLatch latch;
        private KafkaConsumer<String,String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());


        public ConsumerRunnable(CountDownLatch latch, String bootstrapServers, String topic, String groupId) {
            this.latch = latch;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            //In consumer we need to deserialize.
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //This always reset the offset for the group.

            // create consumer
            consumer = new KafkaConsumer<String, String>(properties);
            // subscribe consumer to our topic(s)
            consumer.subscribe(Arrays.asList(topic));
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

