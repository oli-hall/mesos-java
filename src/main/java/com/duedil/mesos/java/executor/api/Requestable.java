package com.duedil.mesos.java.executor.api;

import com.google.api.client.http.HttpRequest;

public interface Requestable {

    public HttpRequest createRequest();

}
