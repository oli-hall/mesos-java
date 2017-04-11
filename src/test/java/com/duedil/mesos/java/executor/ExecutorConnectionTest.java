package com.duedil.mesos.java.executor;

import org.apache.mesos.v1.Protos.ExecutorInfo;
import org.apache.mesos.v1.Protos.FrameworkID;
import org.apache.mesos.v1.Protos.FrameworkInfo;
import org.junit.Test;

import java.net.URISyntaxException;

import static com.duedil.mesos.java.executor.ExecutorConnection.DEFAULT_MAX_RETRIES;
import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.mock;

public class ExecutorConnectionTest {

    @Test
    public void testRetainsValues() throws URISyntaxException {
        FrameworkInfo framework = FrameworkInfo.newBuilder().getDefaultInstanceForType();
        FrameworkID frameworkId = FrameworkID.newBuilder().getDefaultInstanceForType();
        ExecutorInfo executorInfo = ExecutorInfo.newBuilder().getDefaultInstanceForType();
        ActionableListener eventListener = mock(ActionableListener.class);
        int maxRetries = 3;

        ExecutorConnection connection = new ExecutorConnection(framework, frameworkId, executorInfo, eventListener, maxRetries);
        assertEquals(connection.getFramework(), framework);
        assertEquals(connection.getListener(), eventListener);
        assertEquals(connection.getFrameworkId(), frameworkId);
        assertEquals(connection.getExecutorInfo(), executorInfo);
        assertEquals(connection.getMaxRetries(), maxRetries);
    }

    @Test
    public void testUsesDefaultMaxRetries() throws URISyntaxException {
        FrameworkInfo framework = FrameworkInfo.newBuilder().getDefaultInstanceForType();
        FrameworkID frameworkId = FrameworkID.newBuilder().getDefaultInstanceForType();
        ExecutorInfo executorInfo = ExecutorInfo.newBuilder().getDefaultInstanceForType();
        ActionableListener eventListener = mock(ActionableListener.class);

        ExecutorConnection connection = new ExecutorConnection(framework, frameworkId, executorInfo, eventListener);
        assertEquals(connection.getMaxRetries(), DEFAULT_MAX_RETRIES);
    }

}
