/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.omni.exchange;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.http.client.BodyGenerator;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.RequestStats;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.Method;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Wraps Apache HttpClient 5 as io.airlift.http.client.HttpClient.
 * Strictly type conversion — no business logic, no page handling.
 */
public class ApacheHttpClientAdapter implements HttpClient {

    private final CloseableHttpAsyncClient hc5Client;
    private final RequestStats stats = new RequestStats();
    private final long maxContentLength;
    private volatile boolean closed;

    public ApacheHttpClientAdapter(CloseableHttpAsyncClient hc5Client, long maxContentLength) {
        this.hc5Client = requireNonNull(hc5Client, "hc5Client is null");
        this.maxContentLength = maxContentLength;
        this.hc5Client.start();
    }

    @Override
    public <T, E extends Exception> T execute(Request request, ResponseHandler<T, E> handler) throws E {
        try {
            return executeAsync(request, handler).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted", e);
        } catch (java.util.concurrent.ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) throw (RuntimeException) cause;
            throw new RuntimeException(cause);
        }
    }

    @Override
    public <T, E extends Exception> HttpResponseFuture<T> executeAsync(Request request, ResponseHandler<T, E> handler) {
        SettableFuture<T> resultFuture = SettableFuture.create();

        // Convert airlift Request → HC5 SimpleHttpRequest
        URI uri = request.getUri();
        SimpleHttpRequest hc5Request = new SimpleHttpRequest(Method.normalizedValueOf(request.getMethod()), uri);
        for (Map.Entry<String, String> entry : request.getHeaders().entries()) {
            hc5Request.addHeader(entry.getKey(), entry.getValue());
        }

        // Write request body if present
        BodyGenerator bodyGenerator = request.getBodyGenerator();
        if (bodyGenerator != null) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                bodyGenerator.write(baos);
                String contentType = request.getHeader("Content-Type");
                hc5Request.setBody(baos.toByteArray(),
                        contentType != null ? ContentType.parse(contentType) : ContentType.APPLICATION_JSON);
            } catch (Exception e) {
                resultFuture.setException(e);
                return new SettableHttpResponseFuture<>(resultFuture);
            }
        }

        hc5Client.execute(hc5Request, new FutureCallback<SimpleHttpResponse>() {
            @Override
            public void completed(SimpleHttpResponse hc5Response) {
                try {
                    Response response = toAirliftResponse(hc5Response);
                    T result = handler.handle(request, response);
                    resultFuture.set(result);
                } catch (Exception e) {
                    try {
                        T errorResult = handler.handleException(request, e);
                        resultFuture.set(errorResult);
                    } catch (Exception inner) {
                        resultFuture.setException(inner);
                    }
                }
            }

            @Override
            public void failed(Exception ex) {
                try {
                    T errorResult = handler.handleException(request, ex);
                    resultFuture.set(errorResult);
                } catch (Exception inner) {
                    resultFuture.setException(inner);
                }
            }

            @Override
            public void cancelled() {
                resultFuture.cancel(true);
            }
        });

        return new SettableHttpResponseFuture<>(resultFuture);
    }

    @Override
    public RequestStats getStats() { return stats; }

    @Override
    public long getMaxContentLength() { return maxContentLength; }

    @Override
    public void close() {
        closed = true;
        try { hc5Client.close(); } catch (Exception ignored) {}
    }

    @Override
    public boolean isClosed() { return closed; }

    private static Response toAirliftResponse(SimpleHttpResponse hc5Response) {
        int statusCode = hc5Response.getCode();
        ImmutableListMultimap.Builder<HeaderName, String> headers = ImmutableListMultimap.builder();
        for (Header h : hc5Response.getHeaders()) {
            headers.put(HeaderName.of(h.getName()), h.getValue());
        }
        byte[] body = hc5Response.getBodyBytes();
        if (body == null) body = new byte[0];
        byte[] finalBody = body;
        return new Response() {
            @Override public int getStatusCode() { return statusCode; }
            @Override public ListMultimap<HeaderName, String> getHeaders() { return headers.build(); }
            @Override public long getBytesRead() { return finalBody.length; }
            @Override public InputStream getInputStream() { return new ByteArrayInputStream(finalBody); }
        };
    }

    private static class SettableHttpResponseFuture<T>
            extends ForwardingListenableFuture<T>
            implements HttpResponseFuture<T> {
        private final ListenableFuture<T> delegate;
        SettableHttpResponseFuture(ListenableFuture<T> delegate) { this.delegate = delegate; }
        @Override protected ListenableFuture<T> delegate() { return delegate; }
        @Override public String getState() { return delegate.isDone() ? "done" : "waiting"; }
    }
}
