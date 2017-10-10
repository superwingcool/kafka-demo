import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.Stores;

import java.io.IOException;
import java.util.Properties;

public class WordCountTopology {
    public static void main(String[] args) throws IOException {

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount-processor");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19091,localhost:19092,localhost:19093");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("SOURCE", new StringDeserializer(), new StringDeserializer(), "words")
                .addProcessor("WordCountProcessor", WordCountProcessor::new, "SOURCE")
                .addStateStore(Stores.create("Counts").withStringKeys().withLongValues().inMemory().build(), "WordCountProcessor")
                .addSink("Sink", "counts", new StringSerializer(), new LongSerializer(), "WordCountProcessor");

        KafkaStreams streams = new KafkaStreams(builder, properties);
        streams.start();
        System.in.read();
        streams.close();
        streams.cleanUp();
    }
}
