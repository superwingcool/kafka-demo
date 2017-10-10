package com.test.kafka.consumer.group;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

public class KafkaConsumerGroupReBalance extends Thread {

    private final KafkaConsumer consumer;


    public KafkaConsumerGroupReBalance(String servers, String groupId, String topic, String clientId){
        Properties props = getProperties(servers, groupId, clientId);
        Properties consumerConfig = ConsumerConfig.addDeserializerToConfig(props, new StringDeserializer(), new StringDeserializer());
        consumer = new KafkaConsumer(consumerConfig);
        consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
            //重新ReBalance后之前的撤销之前这个Consumer的分区情况
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                partitions.forEach(p -> {
                    System.out.printf("Revoked partition for client %s: %s - %s", clientId, p.topic(), p.partition());
                    System.out.println();
                });

            }
            //重新ReBalance后的给这个Consumer的重新分区情况
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                partitions.forEach(p -> {
                    System.out.printf("Assigned partition for client %s: %s - %s", clientId, p.topic(), p.partition());
                    System.out.println();
                });
            }

        });
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private Properties getProperties(String servers, String groupId, String clientId) {
        Properties props = new Properties();
        //kafka服务器链接
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        //Client Id, Consumer Client ID
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        //GROUP ID，相同的GROUP ID的CONSUMER进行balance和rebalance
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //自动提交OFFSET，新版是在_CONSUMER_TOPIC的TOPIC中保存，压缩
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        //在没有设置OFFSET下，使用怎么样的策略来完成
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        //多长时间提交OFFSET
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        return props;
    }

    @Override
    public void run() {
        try{
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for(ConsumerRecord<String, String> record : records) {
                    System.out.printf("key = %s, value = %s, offset = %s", record.key(), record.value(), record.offset());
                    System.out.println();
                }
            }
        }finally {
            consumer.close();
        }
    }

    public static void main(String[] args) {
        KafkaConsumerGroupReBalance kafkaConsumerGroupTest = new KafkaConsumerGroupReBalance("localhost:19091,localhost:19092,localhost:19093", "groupId_test", "kafka_topic", "clientId_test2");
        kafkaConsumerGroupTest.start();
    }

}
