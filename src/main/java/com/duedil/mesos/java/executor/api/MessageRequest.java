package com.duedil.mesos.java.executor.api;

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
import static org.apache.mesos.v1.executor.Protos.Call.Message;
import static org.apache.mesos.v1.executor.Protos.Call.Type.MESSAGE;

public class MessageRequest extends BaseRequest {

    private static final Logger LOG = LoggerFactory.getLogger(MessageRequest.class);

    private final Message message;
    private final FrameworkInfo framework;
    private final ExecutorInfo executor;

    public MessageRequest(Message message, FrameworkInfo framework, ExecutorInfo executor) {
        this.message = checkNotNull(message);
        this.framework = checkNotNull(framework);
        this.executor = checkNotNull(executor);
    }

    @Override
    public HttpRequest createRequest() {
        Builder call = Call.newBuilder()
                .setType(MESSAGE)
                .setMessage(message)
                .setFrameworkId(framework.getId())
                .setExecutorId(executor.getExecutorId());

        HttpRequest request;
        try {
            request = REQUEST_FACTORY.buildPostRequest(BASE_URL, new ProtoHttpContent(call.build()));
        } catch (IOException e) {
            LOG.error("Failed to build Message request: {}", e.getMessage());
            throw new RuntimeException(e);
        }

        request.setThrowExceptionOnExecuteError(false);

        return request;
    }

    public Message getMessage() {
        return message;
    }

    public FrameworkInfo getFramework() {
        return framework;
    }

    public ExecutorInfo getExecutor() {
        return executor;
    }
}
