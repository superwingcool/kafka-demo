package com.test.producer;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

//发送前处理
public class DemoProducerInterceptor implements ProducerInterceptor<String, String> {


    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        if(Integer.parseInt(record.key()) % 2 == 0){
            return record;
        }
        return null;
    }

    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if(metadata != null){
            System.out.println("partition = " + metadata.partition());
        }
    }

    public void close() {

    }

    public void configure(Map<String, ?> configs) {

    }
}
