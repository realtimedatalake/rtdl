package com.rtdl.sf.piidetection.function;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rtdl.sf.piidetection.types.IncomingMessage;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.io.KafkaEgressMessage;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.CompletableFuture;

/**
 * PII detection stateful function class that implements the StatefulFunction interface.
 * <p>
 * - Checks if the incoming message is a type of IncomingMessage
 * - Checks if the message_type is 'rtdl_205' (control-message)
 * - If the message_type is NOT 'rtdl_205', then identifies SSN and phone number patterns in the value and applies masking
 * - Produces the result into the Kafka topic, 'pii-detection'
 */
public class PiiDetectionFn implements StatefulFunction {

    public static final TypeName PII_TYPE = TypeName.typeNameFromString("com.rtdl.sf.pii/pii-detection");
    public static final TypeName PII_EGRESS = TypeName.typeNameFromString("com.rtdl.sf.pii/egress");
    private static final Logger LOG = LoggerFactory.getLogger(PiiDetectionFn.class);

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) {
        try {
            if (!message.is(IncomingMessage.TYPE)) {
                LOG.error("Unknown type");
                throw new IllegalStateException("Unknown type");
            }

            IncomingMessage incomingMessage = message.as(IncomingMessage.TYPE);
            byte[] byteValue;

            if ("rtdl_205".equalsIgnoreCase(incomingMessage.getMessage_type())) {
                LOG.info("Incoming message type is 'rtdl_205' (control-message).");
                byteValue = message.rawValue().toByteArray();
            } else {
                ObjectMapper mapper = new ObjectMapper();
                String jsonString = mapper.writeValueAsString(incomingMessage);
                String maskedJsonString = new PiiDetector().maskPII(jsonString);
                IncomingMessage outgoingMessage = mapper.readValue(maskedJsonString, IncomingMessage.class);
                byteValue = new ObjectMapper().writeValueAsBytes(outgoingMessage);
            }

            context.send(
                    KafkaEgressMessage.forEgress(PII_EGRESS)
                            .withTopic("pii-detection")
                            .withUtf8Key("message")
                            .withValue(byteValue)
                            .build());

        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            try (PrintWriter pw = new PrintWriter(sw)) {
                e.printStackTrace(pw);
                LOG.error(sw.toString());
            }
        }

        return context.done();
    }
}
