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

public class PiiDetectionFn implements StatefulFunction {

    public static final TypeName PII_TYPE = TypeName.typeNameFromString("com.rtdl.sf.pii/pii-detection");
    public static final TypeName PII_EGRESS = TypeName.typeNameFromString("com.rtdl.sf.pii/egress");
    private static final Logger LOG = LoggerFactory.getLogger(PiiDetectionFn.class);

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) {
        LOG.info("Pii received a message: " + message);

        try {
            if (!message.is(IncomingMessage.TYPE)) {
                LOG.error("Unknown type");
                throw new IllegalStateException("Unknown type");
            }

            IncomingMessage incomingMessage = message.as(IncomingMessage.TYPE);

            ObjectMapper mapper = new ObjectMapper();
            String jsonString = mapper.writeValueAsString(incomingMessage);
            String maskedJsonString = new PiiDetector().maskPII(jsonString);
            IncomingMessage outgoingMessage = mapper.readValue(maskedJsonString, IncomingMessage.class);

            context.send(
                    KafkaEgressMessage.forEgress(PII_EGRESS)
                            .withTopic("pii-detection")
                            .withUtf8Key("message")
                            .withValue(new ObjectMapper().writeValueAsBytes(outgoingMessage))
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
