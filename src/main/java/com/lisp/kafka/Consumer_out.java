package com.lisp.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

public class Consumer_out {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "0609");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String>(props);
        consumer.subscribe(Arrays.asList("first"), new ConsumerRebalanceListener() {
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("=========回收分区============");
                for (TopicPartition partition : partitions) {
                    System.out.println("partition=" + partition);
                }
            }

            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("=========分配分区============");
                for (TopicPartition partition : partitions) {
                    System.out.println("partition=" + partition);
                }
            }
        });

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("offset =" + record.offset() + "key=" +record.key() + "value = "+ record.value());
            }
        }

    }
}
