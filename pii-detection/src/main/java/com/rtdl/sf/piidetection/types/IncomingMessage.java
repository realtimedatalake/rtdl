package com.rtdl.sf.piidetection.types;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.types.SimpleType;
import org.apache.flink.statefun.sdk.java.types.Type;

import java.util.Map;
import java.util.Objects;

/**
 * Type class for IncomingMessage. Objects with this type can be passed transparently between functions.
 */
public class IncomingMessage {

    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * The TYPE definition for IncomingMessage.
     */
    public static final Type<IncomingMessage> TYPE = SimpleType.simpleImmutableTypeFrom(
            TypeName.typeNameFromString("com.rtdl.sf.pii/IncomingMessage"),
            mapper::writeValueAsBytes,
            bytes -> mapper.readValue(bytes, IncomingMessage.class));

    private final String stream_id;
    private final String stream_alt_id;
    private final String message_type;
    private final Map<String, Object> payload;

    /**
     * Instantiates a new Incoming message using Jackson annotations.
     *
     * @param stream_id     the stream id
     * @param stream_alt_id the stream alt id
     * @param message_type  the message type
     * @param payload       the payload
     */
    @JsonCreator
    public IncomingMessage(
            @JsonProperty("stream_id") @JsonInclude(JsonInclude.Include.NON_NULL) String stream_id,
            @JsonProperty("stream_alt_id") @JsonInclude(JsonInclude.Include.NON_NULL) String stream_alt_id,
            @JsonProperty("message_type") @JsonInclude(JsonInclude.Include.NON_NULL) String message_type,
            @JsonProperty("payload") Map<String, Object> payload) {

        this.stream_id = stream_id;
        this.stream_alt_id = stream_alt_id;
        this.message_type = message_type;
        this.payload = Objects.requireNonNull(payload);
    }

    /**
     * Gets stream id.
     *
     * @return the stream id
     */
    public String getStream_id() {
        return stream_id;
    }

    /**
     * Gets stream alt id.
     *
     * @return the stream alt id
     */
    public String getStream_alt_id() {
        return stream_alt_id;
    }

    /**
     * Gets message type.
     *
     * @return the message type
     */
    public String getMessage_type() {
        return message_type;
    }

    /**
     * Gets payload.
     *
     * @return the payload
     */
    public Map<String, Object> getPayload() {
        return this.payload;
    }
}
