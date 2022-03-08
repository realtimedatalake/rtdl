package com.rtdl.sf.piidetection;

import com.rtdl.sf.piidetection.function.PiiDetectionFn;
import io.undertow.Undertow;
import org.apache.flink.statefun.sdk.java.StatefulFunctionSpec;
import org.apache.flink.statefun.sdk.java.StatefulFunctions;
import org.apache.flink.statefun.sdk.java.handler.RequestReplyHandler;
import com.rtdl.sf.piidetection.undertow.UndertowHttpHandler;

public class PiiDetectionAppServer {
    public static void main(String[] args) {

        StatefulFunctionSpec spec =
                StatefulFunctionSpec.builder(PiiDetectionFn.PII_TYPE)
                        .withSupplier(PiiDetectionFn::new)
                        .build();

        final StatefulFunctions functions = new StatefulFunctions();
        functions.withStatefulFunction(spec);

        final RequestReplyHandler requestReplyHandler = functions.requestReplyHandler();

        final Undertow httpServer =
                Undertow.builder()
                        .addHttpListener(5000, "0.0.0.0")
                        .setHandler(new UndertowHttpHandler(requestReplyHandler))
                        .build();
        httpServer.start();
    }
}
