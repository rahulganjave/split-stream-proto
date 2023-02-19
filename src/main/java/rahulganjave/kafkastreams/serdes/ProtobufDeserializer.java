package rahulganjave.kafkastreams.serdes;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class ProtobufDeserializer<T extends MessageLite> implements Deserializer<T> {

    private Parser<T> parser;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        parser = (Parser<T>) configs.get("parser");
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            return parser.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            throw new SerializationException("Failed to deserialize from a protobuf byte array.", e);
        }
    }
}
