package com.duedil.mesos.java.executor.api;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import org.apache.mesos.v1.Protos.ExecutorID;
import org.apache.mesos.v1.Protos.ExecutorInfo;
import org.apache.mesos.v1.Protos.FrameworkID;
import org.apache.mesos.v1.Protos.FrameworkInfo;
import org.apache.mesos.v1.executor.Protos.Call;
import org.apache.mesos.v1.executor.Protos.Call.Subscribe;
import org.apache.mesos.v1.executor.Protos.Call.Update;
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
public class UpdateRequestTest {

    private Update update;
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

        // TODO: Update
    }

    @Test
    public void testConstructorRetainsValues() {
        UpdateRequest call = new UpdateRequest(update, framework, executor);
        assertThat(call, is(not(nullValue())));

        assertThat(call.getSubscription(), is(equalTo(subscription)));
        assertThat(call.getFramework(), is(equalTo(framework)));
        assertThat(call.getExecutor(), is(equalTo(executor)));
    }

    @SuppressWarnings("unused")
    @Test(expected = NullPointerException.class)
    public void testConstructorRequiresNonNullSubscription() {
        SubscribeRequest call = new SubscribeRequest(null, framework, executor);
    }

    @SuppressWarnings("unused")
    @Test(expected = NullPointerException.class)
    public void testConstructorRequiresNonNullFrameworkInfo() {
//        SubscribeRequest call = new SubscribeRequest(subscription, null, executor);
    }

    @SuppressWarnings("unused")
    @Test(expected = NullPointerException.class)
    public void testConstructorRequiresNonNullExecutorInfo() {
//        SubscribeRequest call = new SubscribeRequest(subscription, framework, null);
    }

    @Test
    public void testRequestHasCorrectHeaders() {
//        SubscribeRequest call = new SubscribeRequest(subscription, framework, executor);
//        HttpRequest req = call.createRequest();
//        HttpHeaders headers = req.getHeaders();
//        assertThat(headers.get("Content-type").toString(), is(equalTo("[application/json]")));
//        assertThat(headers.get("Accept").toString(), is(equalTo("[application/json]")));
//        assertThat(headers.get("Accept-encoding").toString(), is(equalTo("[chunked]")));
    }

    @Test
    public void testRequestHasCorrectContent() throws IOException {
//        SubscribeRequest call = new SubscribeRequest(subscription, framework, executor);
//        HttpRequest req = call.createRequest();
//
//        ByteArrayOutputStream out = new ByteArrayOutputStream();
//        req.getContent().writeTo(out);
//        byte[] contentBytes = out.toByteArray();
//        Call contentCall = Call.parseFrom(contentBytes);
//
//        assertThat(contentCall.getFrameworkId(), is(equalTo(framework.getId())));
//        assertThat(contentCall.getExecutorId(), is(equalTo(executor.getExecutorId())));
//        Subscribe payload = contentCall.getSubscribe();
//        assertThat(payload, is(equalTo(subscription)));
    }

}
