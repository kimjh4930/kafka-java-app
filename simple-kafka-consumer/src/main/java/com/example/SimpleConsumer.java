package com.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class SimpleConsumer {
    private final static Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);

    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVER = "my-kafka:9092";
    private final static String GROUP_ID = "test-group";

    private final static int PARTITION_NUMBER = 0;

    private static KafkaConsumer<String, String> consumer;
    private static Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    public static void main(String args[]){
        Runtime.getRuntime().addShutdownHook(new ShutdownThread());

        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

//        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
//        consumer.subscribe(Arrays.asList(TOPIC_NAME));
//
//        while(true){
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
//            for(ConsumerRecord<String, String> record : records){
//                logger.info("{}", record);
//            }
//        }

        //auto_commit false
        //sync offset commit
//        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
//
//        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
//        consumer.subscribe(Arrays.asList(TOPIC_NAME));

//        while(true){
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
//
//            Map<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>();
//
//            for(ConsumerRecord<String, String> record : records){
//                logger.info("record:{}", record);
//                currentOffset.put(
//                        new TopicPartition(record.topic(), record.partition()),
//                        new OffsetAndMetadata(record.offset() + 1, null)
//                );
//                consumer.commitSync(currentOffset);
//            }
//        }

        //async offset commit

        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(Arrays.asList(TOPIC_NAME), new RebalanceListener());

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                currentOffsets = new HashMap<>();

                for (ConsumerRecord<String, String> record : records) {
                    logger.info("{}", record);

                    currentOffsets.put(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1, null)
                    );

                    consumer.commitSync(currentOffsets);
                }
                //callback
                consumer.commitAsync((offsets, e) -> {
                    if (e != null) {
                        System.err.println("commit failed");
                    } else {
                        System.err.println("Commit Succeeded");
                    }

                    if (e != null) {
                        logger.error("Commit failed for offsets {}", offsets, e);
                    }
                });
            }
        } catch (WakeupException e){
            logger.warn("Wakeup consumer");
        } finally {
            consumer.close();
        }

    }

    private static class RebalanceListener implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            logger.warn("Partitions are assigned");
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            logger.warn("Partitions are revoked");
            consumer.commitSync(currentOffsets);
        }
    }

    private static class ShutdownThread extends Thread {
        public void run() {
            logger.info("Shutdown hook");
            consumer.wakeup();
        }
    }
}

