package com.rtdl.sf.piidetection.types;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.types.SimpleType;
import org.apache.flink.statefun.sdk.java.types.Type;

import java.util.Map;
import java.util.Objects;

public class IncomingMessage {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static final Type<IncomingMessage> TYPE = SimpleType.simpleImmutableTypeFrom(
            TypeName.typeNameFromString("com.rtdl.sf.pii/IncomingMessage"),
            mapper::writeValueAsBytes,
            bytes -> mapper.readValue(bytes, IncomingMessage.class));

    private final String streamId;
    private final Map<String, Object> payload;
    private final Map<String, Object> payloadContent;
    private final Map<String, Object> propertiesContent;

    @JsonCreator
    public IncomingMessage(
            @JsonProperty("stream_id") String streamId,
            @JsonProperty("payload") Map<String, Object> payload) {

        this.streamId = Objects.requireNonNull(streamId);
        this.payload = Objects.requireNonNull(payload);
        this.payloadContent = Objects.requireNonNull((Map<String, Object>) this.payload.get("payload"));
        this.propertiesContent = Objects.requireNonNull((Map<String, Object>) this.payloadContent.get("properties"));
    }

    public String getStreamId() {
        return streamId;
    }

    public String getMessageType() {
        return (String) payload.get("message_type");
    }

    public int[] getArray() {
        return (int[]) this.payloadContent.get("array");
    }

    public String getName() {
        return (String) this.payloadContent.get("name");
    }

    public int getAge() {
        return (int) this.propertiesContent.get("age");
    }

    public Map<String, Object> getPayload() {
        return payload;
    }
}
