package com.duedil.mesos.java.executor.api;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.util.Base64;
import com.google.protobuf.ByteString;
import org.apache.mesos.v1.Protos.ExecutorID;
import org.apache.mesos.v1.Protos.ExecutorInfo;
import org.apache.mesos.v1.Protos.FrameworkID;
import org.apache.mesos.v1.Protos.FrameworkInfo;
import org.apache.mesos.v1.executor.Protos.Call;
import org.apache.mesos.v1.executor.Protos.Call.Message;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FrameworkInfo.class, ExecutorInfo.class})
public class MessageRequestTest {

    private static final String MESSAGE_CONTENT_STRING = "this is a test of the emergency broadcast systems";
    private static final byte[] MESSAGE_CONTENT = MESSAGE_CONTENT_STRING.getBytes();

    private Message message;
    private FrameworkInfo framework;
    private ExecutorInfo executor;

    @Before
    public void setUp() {
        framework = PowerMockito.mock(FrameworkInfo.class);
        FrameworkID frameworkId = FrameworkID.newBuilder().setValue("fr4m3w0rk-1d").build();
        when(framework.getId()).thenReturn(frameworkId);

        executor = PowerMockito.mock(ExecutorInfo.class);
        ExecutorID executorId = ExecutorID.newBuilder().setValue("3x3cut0r-1d").build();
        when(executor.getExecutorId()).thenReturn(executorId);

        ByteString data = ByteString.copyFrom(Base64.encodeBase64(MESSAGE_CONTENT));
        message = Message.newBuilder()
                .setData(data)
                .build();
    }

    @Test
    public void testConstructorRetainsValues() {
        MessageRequest request = new MessageRequest(message, framework, executor);
        assertThat(request, is(not(nullValue())));

        assertThat(request.getMessage(), is(equalTo(message)));
        assertThat(request.getFramework(), is(equalTo(framework)));
        assertThat(request.getExecutor(), is(equalTo(executor)));
    }

    @SuppressWarnings("unused")
    @Test(expected = NullPointerException.class)
    public void testConstructorRequiresNonNullMessage() {
        MessageRequest request = new MessageRequest(null, framework, executor);
    }

    @SuppressWarnings("unused")
    @Test(expected = NullPointerException.class)
    public void testConstructorRequiresNonNullFrameworkInfo() {
        MessageRequest request = new MessageRequest(message, null, executor);
    }

    @SuppressWarnings("unused")
    @Test(expected = NullPointerException.class)
    public void testConstructorRequiresNonNullExecutorInfo() {
        MessageRequest request = new MessageRequest(message, framework, null);
    }

    @Test
    public void testRequestHasCorrectHeaders() {
        MessageRequest call = new MessageRequest(message, framework, executor);
        HttpRequest req = call.createRequest();
        HttpHeaders headers = req.getHeaders();
        assertThat(headers.get("Content-type").toString(), is(equalTo("[application/json]"))); // WTF
        assertThat(headers.get("Accept").toString(), is(equalTo("[application/json]")));
    }

    @Test
    public void testRequestHasCorrectContent() throws IOException {
        MessageRequest call = new MessageRequest(message, framework, executor);
        HttpRequest req = call.createRequest();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        req.getContent().writeTo(out);
        byte[] contentBytes = out.toByteArray();
        Call contentCall = Call.parseFrom(contentBytes).toBuilder().build();

        assertThat(contentCall.getFrameworkId(), is(equalTo(framework.getId())));
        assertThat(contentCall.getExecutorId(), is(equalTo(executor.getExecutorId())));
        byte[] decodedData = Base64.decodeBase64(contentCall.getMessage().getData().toByteArray());
        String data = new String(decodedData);
        assertThat(data, is(equalTo(MESSAGE_CONTENT_STRING)));
    }
}
