package rahulganjave.kafkastreams.serdes;

import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

public class ProtobufSerdes<T extends MessageLite> implements Serde<T> {

    private final Serializer<T> serializer;
    private final Deserializer<T> deserializer;

    public ProtobufSerdes(Parser<T> parser) {
        serializer = new ProtobufSerializer<>();
        deserializer = new ProtobufDeserializer<>();
        Map<String, Parser<T>> config = new HashMap<>();
        config.put("parser", parser);
        deserializer.configure(config, false);
    }

    @Override
    public Serializer<T> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return deserializer;
    }
}
