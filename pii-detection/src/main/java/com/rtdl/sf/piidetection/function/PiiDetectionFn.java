package com.rtdl.sf.piidetection.function;

import com.rtdl.sf.piidetection.types.IncomingMessage;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.io.KafkaEgressMessage;
import org.apache.flink.statefun.sdk.java.message.Message;

import java.util.concurrent.CompletableFuture;

public class PiiDetectionFn implements StatefulFunction {

    public static final TypeName PII_TYPE = TypeName.typeNameFromString("com.rtdl.sf/pii-detection");
    public static final TypeName PII_EGRESS = TypeName.typeNameFromString("com.rtdl.sf/pii-egress");

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) {
        if (!message.is(IncomingMessage.TYPE)) {
            throw new IllegalStateException("Unknown type");
        }

        IncomingMessage incomingMessage = message.as(IncomingMessage.TYPE);

        context.send(
                KafkaEgressMessage.forEgress(PII_EGRESS)
                        .withTopic("pii-detection")
                        .withUtf8Value("Testing pii detection... Incoming message:  " + incomingMessage.toString())
                        .build());

        return context.done();
    }
}
