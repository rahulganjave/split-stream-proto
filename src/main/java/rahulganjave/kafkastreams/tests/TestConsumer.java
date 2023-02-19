package rahulganjave.kafkastreams.tests;

import com.google.protobuf.Parser;
import rahulganjave.kafkastreams.proto.ActingOuterClass;
import rahulganjave.kafkastreams.serdes.ProtobufDeserializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
public class TestConsumer {
    public static void main(String[] args) {
        if (args.length < 1) {
            throw new IllegalStateException("Must specify an output topic name.");
        }

        Deserializer<ActingOuterClass.Acting> deserializer = new ProtobufDeserializer<>();
        Map<String, Parser<ActingOuterClass.Acting>> config = new HashMap<>();
        config.put("parser", ActingOuterClass.Acting.parser());
        deserializer.configure(config, false);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (final Consumer<String, ActingOuterClass.Acting> consumer = new KafkaConsumer<>(props, new StringDeserializer(), deserializer)) {
            consumer.subscribe(Arrays.asList(args[0]));
            while (true) {
                ConsumerRecords<String, ActingOuterClass.Acting> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, ActingOuterClass.Acting> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                }
            }
        }
    }
}


//java -cp build/libs/kstreams-transform-standalone-0.0.1-all.jar rahulganjave.kafkastreams.tests.TestConsumer drama-acting-events-proto
//java -cp build/libs/kstreams-transform-standalone-0.0.1-all.jar rahulganjave.kafkastreams.tests.TestConsumer fantasy-acting-events-proto
//java -cp build/libs/kstreams-transform-standalone-0.0.1-all.jar rahulganjave.kafkastreams.tests.TestConsumer other-acting-events-proto
