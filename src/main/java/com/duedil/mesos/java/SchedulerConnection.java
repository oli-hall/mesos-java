package com.duedil.mesos.java;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.http.protobuf.ProtoHttpContent;
import com.google.api.client.protobuf.ProtoObjectParser;

import org.apache.mesos.v1.scheduler.Protos;
import org.apache.mesos.v1.scheduler.Protos.Event;
import com.google.protobuf.util.JsonFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.mesos.v1.Protos.FrameworkInfo;
import org.apache.mesos.v1.Protos.FrameworkID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.duedil.mesos.java.Utils.schedulerEndpoint;
import static org.apache.http.HttpStatus.SC_OK;
import static org.apache.http.HttpStatus.SC_TEMPORARY_REDIRECT;
import static org.apache.mesos.v1.scheduler.Protos.Call;
import static org.apache.mesos.v1.scheduler.Protos.Call.Subscribe;

import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.mesos.v1.scheduler.Protos.Call.Type.SUBSCRIBE;

public class SchedulerConnection extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(SchedulerConnection.class);
    // TODO can this/should this be shared with the main driver?
    private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
    private static final int CONNECT_RETRY_INTERVAL_SECS = 5;

    private final FrameworkInfo framework;
    private final EventListener listener;
    private final URI masterUri;
    private final FrameworkID frameworkId;
    private final int maxRetries;

    public SchedulerConnection(FrameworkInfo framework, EventListener listener, URI masterUri,
                               FrameworkID frameworkId) {
        this(framework, listener, masterUri, frameworkId, 5);
    }

    // TODO need to think about how this is initialised
    public SchedulerConnection(FrameworkInfo framework, EventListener listener, URI masterUri,
                               FrameworkID frameworkId, int maxRetries) {
        this.framework = checkNotNull(framework);
        this.listener = checkNotNull(listener);
        this.masterUri = checkNotNull(masterUri);
        this.frameworkId = frameworkId;
        this.maxRetries = maxRetries;
    }

    @Override
    public void run() {
        // TODO need an escape from this loop to shut the scheduler driver down
        while (true) {
            HttpRequest request = createSubscribeRequest();
            try {
                HttpResponse response = request.execute();

                if (response.getStatusCode() != SC_OK) {
                    // TODO do all bad responses get thrown as HTTP exceptions? if so, this is superfluous
                    throw new RuntimeException(String.format("Received bad response code: %d", response.getStatusCode()));
                }

                String streamId = response.getHeaders().get("Mesos-Stream-Id").toString();
                listener.setStreamId(streamId.substring(1, streamId.length() - 1));
                try {
                    processResponseStream(response);
                } catch (IOException e) {
                    LOG.warn(String.format("Lost connection to master: %s\nRetrying...", e.getMessage()));
                    Thread.sleep(CONNECT_RETRY_INTERVAL_SECS * 1000);
                }
            } catch (HttpResponseException e) {
                if (e.getStatusCode() == SC_TEMPORARY_REDIRECT) {
                    LOG.debug("Redirect received, points to %s" + e.getHeaders().get("Location").toString());
                }
                // TODO err
                // can get a 307 or 404 when re-connecting :/
                throw new RuntimeException(String.format("Error subscribing to Mesos at %s: %d, %s", masterUri.toString(), e.getStatusCode(), e.getStatusMessage()), e);
            } catch (IOException e) {
                // TODO failed when making request somehow
            } catch (InterruptedException e) {
                // TODO sleep interrupted
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
                } while (lengthBytes.length() < 1);

                int messageLength = Integer.valueOf(lengthBytes);
                char[] chars = new char[messageLength];
                int charsRead = r.read(chars, 0, messageLength);

                if (charsRead < messageLength) {
                    throw new RuntimeException(String.format(
                            "Unable to read full message. Expected %d bytes, read %d bytes",
                            messageLength,
                            charsRead));
                }
                String message = String.valueOf(chars);

                Event.Builder builder = Event.newBuilder();
                parser.merge(message, builder);
                Event event = builder.build();

                LOG.debug("Received event:", event.toString());
                listener.onEvent(event);
            }
        }
    }

    private HttpRequest createSubscribeRequest() {
        HttpRequest request;Subscribe.Builder subscribe = Subscribe.newBuilder()
                .setFrameworkInfo(framework);

        Call.Builder subCall = Call.newBuilder()
                .setType(SUBSCRIBE)
                .setSubscribe(subscribe);

        if (frameworkId != null) {
            subCall.setFrameworkId(frameworkId);
        }


        HttpRequestFactory requestFactory = HTTP_TRANSPORT.createRequestFactory(
                new HttpRequestInitializer() {
                    @Override
                    public void initialize(HttpRequest request) {
                        request.setParser(new ProtoObjectParser());
                        HttpHeaders headers = new HttpHeaders();
                        headers.setContentType("application/json");
                        headers.setAccept("application/json");
                        request.setHeaders(headers);
                    }
                });

        try {
            GenericUrl url = new GenericUrl(schedulerEndpoint(this.masterUri));
            request = requestFactory.buildPostRequest(url, new ProtoHttpContent(subCall.build()));
        } catch (IOException e) {
            throw new RuntimeException("Error building Subscribe request", e);
        }
        return request;
    }
}
