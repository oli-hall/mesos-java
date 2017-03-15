package com.duedil.mesos.java;

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
import static org.apache.mesos.v1.scheduler.Protos.Call;
import static org.apache.mesos.v1.scheduler.Protos.Call.Subscribe;

import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.mesos.v1.scheduler.Protos.Call.Type.SUBSCRIBE;

public class SchedulerConnection extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(SchedulerConnection.class);
    // TODO can this/should this be shared with the main driver?
    private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();

    private final FrameworkInfo framework;
    private final EventListener listener;
    private final URI masterUri;
    private final FrameworkID frameworkId;

    // TODO need to think about how this is initialised
    public SchedulerConnection(FrameworkInfo framework, EventListener listener, URI masterUri,
                               FrameworkID frameworkId) {
        this.framework = checkNotNull(framework);
        this.listener = checkNotNull(listener);
        this.masterUri = checkNotNull(masterUri);
        this.frameworkId = frameworkId;
        // TODO threading!
    }

    @Override
    public void run() {
        Subscribe.Builder subscribe = Subscribe.newBuilder()
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
            HttpRequest request = requestFactory.buildPostRequest(url, new ProtoHttpContent(subCall.build()));

            HttpResponse response = request.execute();

            if (response.getStatusCode() != SC_OK) {
                // TODO probably don't throw a runtime here
                throw new RuntimeException(String.format("Received bad response code: %d", response.getStatusCode()));
            }

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
        } catch (IOException e) {
            // TODO handle this and carry on?
            throw new RuntimeException(e);
        }
    }
}
