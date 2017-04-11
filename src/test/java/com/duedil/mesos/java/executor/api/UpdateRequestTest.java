package com.duedil.mesos.java.executor.api;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import org.apache.mesos.v1.Protos.ExecutorID;
import org.apache.mesos.v1.Protos.ExecutorInfo;
import org.apache.mesos.v1.Protos.FrameworkID;
import org.apache.mesos.v1.Protos.FrameworkInfo;
import org.apache.mesos.v1.Protos.TaskID;
import org.apache.mesos.v1.Protos.TaskState;
import org.apache.mesos.v1.Protos.TaskStatus;
import org.apache.mesos.v1.executor.Protos.Call;
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

        update = Update.newBuilder()
                .setStatus(TaskStatus.newBuilder()
                        .setTaskId(TaskID.newBuilder().setValue("t4sk-1d").build())
                        .setState(TaskState.TASK_FINISHED)
                        .build())
                .build();
    }

    @Test
    public void testConstructorRetainsValues() {
        UpdateRequest call = new UpdateRequest(update, framework, executor);
        assertThat(call, is(not(nullValue())));

        assertThat(call.getUpdate(), is(equalTo(update)));
        assertThat(call.getFramework(), is(equalTo(framework)));
        assertThat(call.getExecutor(), is(equalTo(executor)));
    }

    @SuppressWarnings("unused")
    @Test(expected = NullPointerException.class)
    public void testConstructorRequiresNonNullUpdate() {
        UpdateRequest call = new UpdateRequest(null, framework, executor);
    }

    @SuppressWarnings("unused")
    @Test(expected = NullPointerException.class)
    public void testConstructorRequiresNonNullFrameworkInfo() {
        UpdateRequest call = new UpdateRequest(update, null, executor);
    }

    @SuppressWarnings("unused")
    @Test(expected = NullPointerException.class)
    public void testConstructorRequiresNonNullExecutorInfo() {
        UpdateRequest call = new UpdateRequest(update, framework, null);
    }

    @Test
    public void testRequestHasCorrectHeaders() {
        UpdateRequest call = new UpdateRequest(update, framework, executor);
        HttpRequest req = call.createRequest();
        HttpHeaders headers = req.getHeaders();
        assertThat(headers.get("Content-type").toString(), is(equalTo("[application/json]")));
        assertThat(headers.get("Accept").toString(), is(equalTo("[application/json]")));
    }

    @Test
    public void testRequestHasCorrectContent() throws IOException {
        UpdateRequest call = new UpdateRequest(update, framework, executor);
        HttpRequest req = call.createRequest();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        req.getContent().writeTo(out);
        byte[] contentBytes = out.toByteArray();
        Call contentCall = Call.parseFrom(contentBytes);

        assertThat(contentCall.getFrameworkId(), is(equalTo(framework.getId())));
        assertThat(contentCall.getExecutorId(), is(equalTo(executor.getExecutorId())));
        Update payload = contentCall.getUpdate();
        assertThat(payload, is(equalTo(update)));
    }

}
