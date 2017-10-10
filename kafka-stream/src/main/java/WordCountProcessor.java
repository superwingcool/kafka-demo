import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Optional;
import java.util.stream.Stream;

public class WordCountProcessor implements Processor<String, String> {

    private ProcessorContext context;
    private KeyValueStore<String, Long> kvStore;

    public void init(ProcessorContext context) {
        this.context = context;
        this.context.schedule(1000);
        this.kvStore = (KeyValueStore<String, Long>) context.getStateStore("Counts");
    }

    public void process(String key, String value) {
        Stream.of(value.toLowerCase().split(" ")).forEach(word -> {
            Optional<Long> counts = Optional.ofNullable(kvStore.get(word));
            Long count = counts.map(wordCount -> wordCount + 1).orElse(1l);
            kvStore.put(word, count);
        });
    }

    public void punctuate(long l) {
        KeyValueIterator<String, Long> iterator = this.kvStore.all();
        iterator.forEachRemaining(entry -> {
            context.forward(entry.key, entry.value);
            kvStore.delete(entry.key);
        });
        context.commit();

    }

    public void close() {
        kvStore.close();
    }
}
