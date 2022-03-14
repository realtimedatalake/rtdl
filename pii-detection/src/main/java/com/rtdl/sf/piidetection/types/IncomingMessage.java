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

    private final String stream_id;
    private final Map<String, Object> payload;

    @JsonCreator
    public IncomingMessage(
            @JsonProperty("stream_id") String stream_id,
            @JsonProperty("payload") Map<String, Object> payload) {

        this.stream_id = Objects.requireNonNull(stream_id);
        this.payload = Objects.requireNonNull(payload);
    }

    public String getStream_id() {
        return stream_id;
    }

    public Map<String, Object> getPayload() {
        return this.payload;
    }
}
