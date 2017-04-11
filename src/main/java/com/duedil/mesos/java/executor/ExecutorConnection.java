package com.duedil.mesos.java.executor;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.http.protobuf.ProtoHttpContent;
import com.google.api.client.protobuf.ProtoObjectParser;
import com.google.protobuf.util.JsonFormat;
import org.apache.mesos.v1.Protos.ExecutorInfo;
import org.apache.mesos.v1.Protos.FrameworkID;
import org.apache.mesos.v1.Protos.TaskInfo;
import org.apache.mesos.v1.Protos.FrameworkInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import static com.duedil.mesos.java.Utils.executorEndpoint;
import static com.google.api.client.util.Preconditions.checkNotNull;
import static org.apache.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.http.HttpStatus.SC_OK;
import static org.apache.mesos.v1.executor.Protos.Call;
import static org.apache.mesos.v1.executor.Protos.Call.Subscribe;
import static org.apache.mesos.v1.executor.Protos.Call.Type.SUBSCRIBE;
import static org.apache.mesos.v1.executor.Protos.Call.Update;
import static org.apache.mesos.v1.executor.Protos.Event;



public class ExecutorConnection extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutorConnection.class);
    private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();

    private static final Set<TaskInfo> UNACKNOWLEDGED_TASKS = new HashSet<>();
    private static final Set<Update> UNACKNOWLEDGED_UPDATES = new HashSet<>();

    private final ActionableListener listener;
    private final FrameworkInfo framework;
    private final FrameworkID frameworkId;
    private final ExecutorInfo executorInfo;
    private final int maxRetries;
    static final int DEFAULT_MAX_RETRIES = 5;
    private int retries = 0;

    public ExecutorConnection(FrameworkInfo framework, FrameworkID frameworkId, ExecutorInfo executorInfo, ActionableListener listener) {
        this(framework, frameworkId, executorInfo, listener, DEFAULT_MAX_RETRIES);
    }

    public ExecutorConnection(FrameworkInfo framework, FrameworkID frameworkId, ExecutorInfo executorInfo, ActionableListener listener, int maxRetries) {
        this.framework = checkNotNull(framework);
        this.frameworkId = checkNotNull(frameworkId);
        this.executorInfo = checkNotNull(executorInfo);
        this.listener = checkNotNull(listener);
        this.maxRetries = maxRetries;
    }

    @Override
    public void run() {
        while (true) {
            HttpRequest request = createSubscribeRequest();

            try {
                HttpResponse response = request.execute();

                int statusCode = response.getStatusCode();
                if (statusCode == SC_NOT_FOUND) {
                    backoff();
                    continue;
                }
                else if (statusCode != SC_OK) {
                    throw new RuntimeException(String.format("Received bad response code: %d", statusCode));
                }
                resetRetries();

                try {
                    processResponseStream(response);
                }
                catch (IOException e) {
                    LOG.warn("Lost connection to agent: {}. Retrying...", e.getMessage());
                    backoff();
                }
            } catch (IOException e) {
                LOG.error("Error during SUBSCRIBE: {}", e.getMessage());
                backoff();
            }
        }
    }

    private void processResponseStream(HttpResponse response) throws IOException {
        try (InputStream content = response.getContent()) {
            BufferedReader r = new BufferedReader(new InputStreamReader(content));
            JsonFormat.Parser parser = JsonFormat.parser();

            while (true) {
                String lengthBytes;
                do {
                    lengthBytes = r.readLine();
                    if (lengthBytes == null) {
                        LOG.info("Stream terminated from remote end.");
                        return;
                    }
                } while (lengthBytes.length() < 1);

                int messageLength = Integer.valueOf(lengthBytes);
                char[] chars = new char[messageLength];
                int charsRead = r.read(chars, 0, messageLength);

                if (charsRead < messageLength) {
                    LOG.error("Incomplete message, expected {} bytes, read {} bytes", messageLength, charsRead);
                    throw new RuntimeException(String.format(
                            "Unable to read full message. Expected %d bytes, read %d bytes",
                            messageLength,
                            charsRead
                    ));
                }
                String message = String.valueOf(chars);

                Event.Builder builder = Event.newBuilder();
                parser.merge(message, builder);
                Event event = builder.build();
                listener.onEvent(event);
            }
        }
    }

    private HttpRequest createSubscribeRequest() {
        HttpRequest request;
        Subscribe subscribe = Subscribe.newBuilder()
                .addAllUnacknowledgedTasks(UNACKNOWLEDGED_TAKS)
                .addAllUnacknowledgedUpdates(UNACKNOWLEDGED_UPDATES)
                .build();

        Call.Builder call = Call.newBuilder()
                .setType(SUBSCRIBE)
                .setSubscribe(subscribe)
                .setFrameworkId(frameworkId)
                .setExecutorId(executorInfo.getExecutorId());

        HttpRequestFactory requestFactory = HTTP_TRANSPORT.createRequestFactory(new HttpRequestInitializer() {
            @Override
            public void initialize(HttpRequest request) throws IOException {
                request.setParser(new ProtoObjectParser());
                HttpHeaders headers = new HttpHeaders();
                headers.setContentType("application/json");
                headers.setAccept("application/json");
                request.setHeaders(headers);
            }
        });

        // TODO: figure out the port dynamically
//        executorInfo.getDiscovery().getPorts().getPorts(0);
        GenericUrl url = new GenericUrl(executorEndpoint());
        try {
            request = requestFactory.buildPostRequest(url, new ProtoHttpContent(call.build()));
        } catch (IOException e) {
            LOG.error("Failed to build Subscribe request: {}", e.getMessage());
            throw new RuntimeException(e);
        }

        request.setThrowExceptionOnExecuteError(false);
        return request;
    }

    private void backoff() {
        if (++retries >= maxRetries) {
            LOG.error("Number of max retries ({}) exceeded, aborting", maxRetries);
            throw new RuntimeException(String.format("Backed off more than %d times, aborting.", maxRetries));
        }
        try {
            Thread.sleep((long)(Math.pow(2, retries) * 1000));
        } catch (InterruptedException e) {
            LOG.error("Backoff interrupted: {}", e.getMessage());
        }
    }

    private void resetRetries() {
        retries = 0;
    }

    public static Set<TaskInfo> getUnacknowledgedTasks() {
        return UNACKNOWLEDGED_TASKS;
    }

    public static Set<Update> getUnacknowledgedUpdates() {
        return UNACKNOWLEDGED_UPDATES;
    }

    public ActionableListener getListener() {
        return listener;
    }

    public FrameworkInfo getFramework() {
        return framework;
    }

    public FrameworkID getFrameworkId() {
        return frameworkId;
    }

    public ExecutorInfo getExecutorInfo() {
        return executorInfo;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

}
