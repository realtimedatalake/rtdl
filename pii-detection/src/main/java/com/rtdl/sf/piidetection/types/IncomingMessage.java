package com.rtdl.sf.piidetection.types;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.types.SimpleType;
import org.apache.flink.statefun.sdk.java.types.Type;

import java.util.Objects;

public class IncomingMessage {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static final Type<IncomingMessage> TYPE = SimpleType.simpleImmutableTypeFrom(
            TypeName.typeNameFromString("com.rtdl.sf/IncomingMessage"),
            mapper::writeValueAsBytes,
            bytes -> mapper.readValue(bytes, IncomingMessage.class));

    private final String streamId;
    private final String messageType;
    private final String payload;

    @JsonCreator
    public IncomingMessage(
            @JsonProperty("stream_id") String streamId,
            @JsonProperty("message_type") String messageType,
            @JsonProperty("payload") String payload) {

        this.streamId = Objects.requireNonNull(streamId);
        this.messageType = Objects.requireNonNull(messageType);
        this.payload = Objects.requireNonNull(payload);
    }

    public String getStreamId() {
        return streamId;
    }

    public String getMessageType() {
        return messageType;
    }

    public String getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "IncomingMessage{" +
                "streamId='" + streamId + '\'' +
                ", messageType='" + messageType + '\'' +
                ", payload='" + payload + '\'' +
                '}';
    }
}
