package rahulganjave.kafkastreams.serdes;


import rahulganjave.kafkastreams.proto.ActingOuterClass;
import rahulganjave.kafkastreams.serdes.ProtobufSerdes;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
public class SplitMovieStreamApp {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("Config file path must be specified.");
        }

        SplitMovieStreamApp app = new SplitMovieStreamApp();
        Properties envProps = app.loadEnvProperties(args[0]);
        Properties streamProps = app.createStreamsProperties(envProps);
        Topology topology = app.buildTopology(envProps);

        app.preCreateTopics(envProps);

        final KafkaStreams streams = new KafkaStreams(topology, streamProps);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Exception e) {
            System.exit(1);
        }
        System.exit(0);
    }

    private Topology buildTopology(Properties envProps) {
        final StreamsBuilder builder = new StreamsBuilder();
        final String inputTopic = envProps.getProperty("input.topic.name");

        KStream<String, ActingOuterClass.Acting>[] branches = builder
                .stream(inputTopic, Consumed.with(Serdes.String(), actingProtobufSerdes()))
                .branch((key, value) -> "drama".equalsIgnoreCase(value.getGenre()),
                        (key, value) -> "fantasy".equalsIgnoreCase(value.getGenre()),
                        (key, value) -> true);
        branches[0].to(envProps.getProperty("output.drama.topic.name"));
        branches[1].to(envProps.getProperty("output.fantasy.topic.name"));
        branches[2].to(envProps.getProperty("output.other.topic.name"));

        return builder.build();
    }

    /**
     * For Kafka Streams required for program build Properties example
     *
     * @param envProps
     * @return
     */
    private Properties createStreamsProperties(Properties envProps) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("application.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }

    /**
     * pre-created input/output topic，If topic, Ignore if it exists
     *
     * @param envProps
     * @throws Exception
     */
    private void preCreateTopics(Properties envProps) throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        String inputTopic = envProps.getProperty("input.topic.name");
        String outputTopic1 = envProps.getProperty("output.drama.topic.name");
        String outputTopic2 = envProps.getProperty("output.fantasy.topic.name");
        String outputTopic3 = envProps.getProperty("output.other.topic.name");
        try (AdminClient client = AdminClient.create(config)) {
            Collection<TopicListing> existingTopics = client.listTopics().listings().get();

            List<NewTopic> topics = new ArrayList<>();
            List<String> topicNames = existingTopics.stream().map(TopicListing::name).collect(Collectors.toList());
            if (!topicNames.contains(inputTopic))
                topics.add(new NewTopic(
                        envProps.getProperty("input.topic.name"),
                        Integer.parseInt(envProps.getProperty("input.topic.partitions")),
                        Short.parseShort(envProps.getProperty("input.topic.replication.factor"))));

            if (!topicNames.contains(outputTopic1))
                topics.add(new NewTopic(
                        envProps.getProperty("output.drama.topic.name"),
                        Integer.parseInt(envProps.getProperty("output.drama.topic.partitions")),
                        Short.parseShort(envProps.getProperty("output.drama.topic.replication.factor"))));

            if (!topicNames.contains(outputTopic2))
                topics.add(new NewTopic(
                        envProps.getProperty("output.fantasy.topic.name"),
                        Integer.parseInt(envProps.getProperty("output.fantasy.topic.partitions")),
                        Short.parseShort(envProps.getProperty("output.fantasy.topic.replication.factor"))));

            if (!topicNames.contains(outputTopic3))
                topics.add(new NewTopic(
                        envProps.getProperty("output.other.topic.name"),
                        Integer.parseInt(envProps.getProperty("output.other.topic.partitions")),
                        Short.parseShort(envProps.getProperty("output.other.topic.replication.factor"))));

            if (!topics.isEmpty())
                client.createTopics(topics).all().get();
        }
    }

    /**
     * load configuration: configuration file under
     *
     * @param fileName
     * @return
     * @throws IOException
     */
    private Properties loadEnvProperties(String fileName) throws IOException {
        Properties envProps = new Properties();
        try (FileInputStream input = new FileInputStream(fileName)) {
            envProps.load(input);
        }
        return envProps;
    }

    /**
     * Construct topic needed Serdes
     *
     * @return
     */
    private static ProtobufSerdes<ActingOuterClass.Acting> actingProtobufSerdes() {
        return new ProtobufSerdes<>(ActingOuterClass.Acting.parser());
    }
}


//java -jar build/libs/kstreams-transform-standalone-0.0.1-all.jar configuration/dev.properties