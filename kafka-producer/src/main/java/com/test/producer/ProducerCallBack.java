package com.test.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * 异步的回调函数
 * 必须实现CallBack接口
 */
public class ProducerCallBack implements Callback {
    private String key;
    private String message;

    public ProducerCallBack(String key, String message) {
        this.key = key;
        this.message = message;
    }

    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        //这里处理发送完成后的逻辑
        if (recordMetadata != null) {
            System.out.println("message key = " + key + ", content = " + message +
                    ", partition = " + recordMetadata.partition() + ", offset = " + recordMetadata.offset());
        } else {
            e.printStackTrace();
        }
    }
}
