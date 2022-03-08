package com.rtdl.sf.piidetection.undertow;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.apache.flink.statefun.sdk.java.handler.RequestReplyHandler;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.slice.Slices;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class UndertowHttpHandler implements HttpHandler {

    private final RequestReplyHandler handler;

    public UndertowHttpHandler(RequestReplyHandler handler) {
        this.handler = Objects.requireNonNull(handler);
    }

    @Override
    public void handleRequest(HttpServerExchange httpServerExchange) {
        httpServerExchange.getRequestReceiver().receiveFullBytes(this::onRequestBody);
    }

    private void onRequestBody(HttpServerExchange httpServerExchange, byte[] requestBytes) {
        httpServerExchange.dispatch();
        CompletableFuture<Slice> future = handler.handle(Slices.wrap(requestBytes));
        future.whenComplete((response, exception) -> onComplete(httpServerExchange, response, exception));
    }

    private void onComplete(HttpServerExchange httpServerExchange, Slice responseBytes, Throwable ex) {
        if (ex != null) {
            ex.printStackTrace(System.out);
            httpServerExchange.getResponseHeaders().put(Headers.STATUS, 500);
            httpServerExchange.endExchange();
            return;
        }
        httpServerExchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/octet-stream");
        httpServerExchange.getResponseSender().send(responseBytes.asReadOnlyByteBuffer());
    }
}
