package com.duedil.mesos.java.executor.api;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.protobuf.ProtoHttpContent;
import org.apache.mesos.v1.Protos.ExecutorInfo;
import org.apache.mesos.v1.Protos.FrameworkInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.mesos.v1.executor.Protos.Call;
import static org.apache.mesos.v1.executor.Protos.Call.Builder;
import static org.apache.mesos.v1.executor.Protos.Call.Subscribe;
import static org.apache.mesos.v1.executor.Protos.Call.Type.SUBSCRIBE;

public class SubscribeRequest extends BaseRequest {

    private static final Logger LOG = LoggerFactory.getLogger(SubscribeRequest.class);

    private static final String ACCEPT_ENCODING = "chunked";

    private final Subscribe subscription;
    private final FrameworkInfo framework;
    private final ExecutorInfo executor;

    public SubscribeRequest(Subscribe subscription, FrameworkInfo framework, ExecutorInfo executor) {
        this.subscription = checkNotNull(subscription);
        this.framework = checkNotNull(framework);
        this.executor = checkNotNull(executor);
    }

    @Override
    public HttpRequest createRequest() {
        Builder call = Call.newBuilder()
                .setType(SUBSCRIBE)
                .setSubscribe(subscription)
                .setFrameworkId(framework.getId())
                .setExecutorId(executor.getExecutorId());

        HttpRequest request;
        try {
            request = REQUEST_FACTORY.buildPostRequest(BASE_URL, new ProtoHttpContent(call.build()));
        } catch (IOException e) {
            LOG.error("Failed to build Subscribe request: {}", e.getMessage());
            throw new RuntimeException(e);
        }

        HttpHeaders newHeaders = request.getHeaders().clone();
        newHeaders.setAcceptEncoding(ACCEPT_ENCODING);
        request.setHeaders(newHeaders);
        request.setThrowExceptionOnExecuteError(false);

        return request;
    }

    public Subscribe getSubscription() {
        return subscription;
    }

    public FrameworkInfo getFramework() {
        return framework;
    }

    public ExecutorInfo getExecutor() {
        return executor;
    }
}
