package rahulganjave.kafkastreams.serdes;

import com.google.protobuf.MessageLite;
import org.apache.kafka.common.serialization.Serializer;

public class ProtobufSerializer<T extends MessageLite> implements Serializer<T> {
    @Override
    public byte[] serialize(String topic, T data) {
        return data == null ? new byte[0] : data.toByteArray();
    }
}
