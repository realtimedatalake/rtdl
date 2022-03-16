package com.rtdl.sf.piidetection;

import com.rtdl.sf.piidetection.function.PiiDetectionFn;
import com.rtdl.sf.piidetection.undertow.UndertowHttpHandler;
import io.undertow.Undertow;
import org.apache.flink.statefun.sdk.java.StatefulFunctionSpec;
import org.apache.flink.statefun.sdk.java.StatefulFunctions;
import org.apache.flink.statefun.sdk.java.handler.RequestReplyHandler;

/**
 * Pii detection main class.
 * <p>
 * - Defines stateful function type - PII_TYPE
 * - Registers statefun function
 * - Obtains a request-reply handler based on the spec
 * - Builds HTTP server that hands off the request-body to the handler
 */
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
