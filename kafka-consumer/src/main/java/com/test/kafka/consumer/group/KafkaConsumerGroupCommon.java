package com.test.kafka.consumer.group;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerGroupCommon extends Thread {

    private final KafkaConsumer consumer;

    public KafkaConsumerGroupCommon(String servers, String groupId, String topic){
        Properties props = getProperties(servers, groupId);
        Properties consumerConfig = ConsumerConfig.addDeserializerToConfig(props, new StringDeserializer(), new LongDeserializer());
        consumer = new KafkaConsumer(consumerConfig);
        consumer.subscribe(Arrays.asList(topic));
    }

    private Properties getProperties(String servers, String groupId) {
        Properties props = new Properties();
        //kafka服务器链接
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
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
                ConsumerRecords<String, Long> records = consumer.poll(1000);
                for(ConsumerRecord<String, Long> record : records) {
                    System.out.printf("key = %s, value = %d, offset = %s", record.key(), record.value(), record.offset());
                    System.out.println();
                }
            }
        }finally {
            consumer.close();
        }
    }

    public static void main(String[] args) {
        KafkaConsumerGroupCommon kafkaConsumerGroupTest = new KafkaConsumerGroupCommon("localhost:19091,localhost:19092,localhost:19093", "group_test", "counts");
        kafkaConsumerGroupTest.start();
    }

}
