package com.duedil.mesos.java.executor;

import org.apache.mesos.v1.Protos.ExecutorInfo;
import org.apache.mesos.v1.Protos.FrameworkID;
import org.apache.mesos.v1.Protos.FrameworkInfo;
import org.junit.Before;
import org.junit.Test;

import java.net.URISyntaxException;

import static com.duedil.mesos.java.executor.ExecutorConnection.DEFAULT_MAX_RETRIES;
import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.mock;

public class ExecutorConnectionTest {

    private FrameworkInfo framework;
    private FrameworkID frameworkId;
    private ExecutorInfo executorInfo;
    private ActionableListener eventListener;

    @Before
    public void setUp() {
        framework = FrameworkInfo.newBuilder().getDefaultInstanceForType();
        frameworkId = FrameworkID.newBuilder().getDefaultInstanceForType();
        executorInfo = ExecutorInfo.newBuilder().getDefaultInstanceForType();
        eventListener = mock(ActionableListener.class);
    }

    @Test
    public void testRetainsValues() throws URISyntaxException {
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
        ExecutorConnection connection = new ExecutorConnection(framework, frameworkId, executorInfo, eventListener);
        assertEquals(connection.getMaxRetries(), DEFAULT_MAX_RETRIES);
    }

    @Test
    public void testNewExecutorConnectionHoldsNoTasksOrUpdates(){
        ExecutorConnection connection = new ExecutorConnection(framework, frameworkId, executorInfo, eventListener);
        assertEquals(connection.getUnacknowledgedUpdates().size(), 0);
        assertEquals(connection.getUnacknowledgedTasks().size(), 0);
    }

}
