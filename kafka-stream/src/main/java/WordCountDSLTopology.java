import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

public class WordCountDSLTopology  {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount-processor");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19091,localhost:19092,localhost:19093");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> words = builder.stream("words");
        KTable<String, Long> wordCounts = words
                .flatMapValues(new ValueMapper<String, Iterable<String>>() {
                    @Override
                    public Iterable<String> apply(String word) {
                        return Arrays.asList(word.toLowerCase().split(" "));
                    }
                })
                .groupBy(new KeyValueMapper<String, String, String>() {
                    @Override
                    public String apply(String key, String word) {
                        return word;
                    }
                })
                .count("Counts");
        wordCounts.to(Serdes.String(), Serdes.Long(), "counts");

        KafkaStreams streams = new KafkaStreams(builder, properties);
        streams.start();
    }
}
