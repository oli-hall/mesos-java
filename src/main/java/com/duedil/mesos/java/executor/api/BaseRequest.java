package com.duedil.mesos.java.executor.api;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.protobuf.ProtoObjectParser;

import java.io.IOException;

import static com.duedil.mesos.java.Utils.executorEndpoint;

public abstract class BaseRequest implements Requestable {

    protected static final HttpRequestFactory REQUEST_FACTORY = new NetHttpTransport()
            .createRequestFactory(new HttpRequestInitializer() {
                @Override
                public void initialize(HttpRequest request) throws IOException {
                    request.setParser(new ProtoObjectParser());
                    HttpHeaders headers = new HttpHeaders();
                    headers.setContentType(CONTENT_TYPE);
                    headers.setAccept(CONTENT_TYPE);
                    request.setHeaders(headers);
                }});
    protected static final GenericUrl BASE_URL = new GenericUrl(executorEndpoint());
    protected static final String CONTENT_TYPE = "application/json";
}
