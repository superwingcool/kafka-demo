package com.test.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerGroupTest {

    private KafkaProducer<String, String> producer;
    private boolean isAsync = true;
    private final String topic;

    public KafkaProducerGroupTest(String servers, String clientId, boolean isAsync, String topic) {
        //kafka通过Properties获取Producer的配置
        Properties props = getProperties(servers, clientId);
        //设置Producer需要发送消息的key和value的Serializer的方式
        //这里使用String
        Properties config = ProducerConfig.addSerializerToConfig(props, new StringSerializer(), new StringSerializer());
        //new Kafka Producer的对象
        this.producer = new KafkaProducer(config);
        //同步发送or异步发送
        this.isAsync = isAsync;
        //设置Topic
        this.topic = topic;
    }

    public void send() throws InterruptedException {
        int messageNo = 1;
        while (true) {
            String message = "Message_" + messageNo;
            if (isAsync) {
                //异步发送
                //使用回调函数ProducerCallBack获取这条消息是否发送正常
                producer.send(new ProducerRecord(topic, String.valueOf(messageNo), message),
                        new ProducerCallBack(String.valueOf(messageNo), message));
            } else {
                try {
                    //同步发送，得到broker确认一直会阻塞
                    RecordMetadata recordMetadata = (RecordMetadata) producer.send(
                            new ProducerRecord(topic, String.valueOf(messageNo), message)).get();
                    System.out.println("message key = " + messageNo + ", content = " + message);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            Thread.sleep(10000);
            messageNo++;
        }
    }

    private Properties getProperties(String servers, String clientId) {
        Properties props = new Properties();
        //kafka集群服务的地址和端口
        props.put("bootstrap.servers", servers);
        //Producer的客户端ID，不配的话默认是clientId = "producer-" + PRODUCER_CLIENT_ID_SEQUENCE.getAndIncrement();
        props.put("client.id", clientId);
        //kafka Producer的发送队列达到这个长度就批量将这个队列的消息发送
        props.put("batch.size", 16384);
        //producer可以用来缓存数据的内存大小。
        //如果数据产生速度大于向broker发送的速度，producer会阻塞或者抛出异常，以“block.on.buffer.full”来表明。
        props.put("buffer.memory", 33554432);
        //请求的最大字节数。即每条消息的最大长度（字节）
        props.put("max.request.size", 1028576);
        return props;
    }

    public static void main(String[] args) throws InterruptedException {

        KafkaProducerGroupTest kafkaProducerGroup = new KafkaProducerGroupTest("localhost:19091,localhost:19092,localhost:19093",
                "centId", true, "kafka_topic");
        kafkaProducerGroup.send();
    }

}
