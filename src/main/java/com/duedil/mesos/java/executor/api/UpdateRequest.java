package com.duedil.mesos.java.executor.api;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.protobuf.ProtoHttpContent;
import org.apache.mesos.v1.Protos.ExecutorInfo;
import org.apache.mesos.v1.Protos.FrameworkInfo;
import org.apache.mesos.v1.executor.Protos.Call;
import org.apache.mesos.v1.executor.Protos.Call.Builder;
import org.apache.mesos.v1.executor.Protos.Call.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.mesos.v1.executor.Protos.Call.Type.UPDATE;
import static com.google.common.base.Preconditions.checkNotNull;

public class UpdateRequest extends BaseRequest {

    private static final Logger LOG = LoggerFactory.getLogger(UpdateRequest.class);

    private final Update update;
    private final FrameworkInfo framework;
    private final ExecutorInfo executor;

    public UpdateRequest(Update update, FrameworkInfo framework, ExecutorInfo executor) {
        this.update = checkNotNull(update);
        this.framework = checkNotNull(framework);
        this.executor = checkNotNull(executor);
    }

    @Override
    public HttpRequest createRequest() {
        Builder call = Call.newBuilder()
                .setType(UPDATE)
                .setUpdate(update)
                .setFrameworkId(framework.getId())
                .setExecutorId(executor.getExecutorId());

        HttpRequest request;
        try {
            request = REQUEST_FACTORY.buildPostRequest(BASE_URL, new ProtoHttpContent(call.build()));
        } catch (IOException e) {
            LOG.error("Failed to build Update request: {}", e.getMessage());
            throw new RuntimeException(e);
        }

        request.setThrowExceptionOnExecuteError(false);

        return request;
    }

    public Update getUpdate() {
        return update;
    }

    public FrameworkInfo getFramework() {
        return framework;
    }

    public ExecutorInfo getExecutor() {
        return executor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UpdateRequest that = (UpdateRequest) o;

        if (update != null ? !update.equals(that.update) : that.update != null) return false;
        if (framework != null ? !framework.equals(that.framework) : that.framework != null) return false;
        return executor != null ? executor.equals(that.executor) : that.executor == null;
    }

    @Override
    public int hashCode() {
        int result = update != null ? update.hashCode() : 0;
        result = 31 * result + (framework != null ? framework.hashCode() : 0);
        result = 31 * result + (executor != null ? executor.hashCode() : 0);
        return result;
    }
}
