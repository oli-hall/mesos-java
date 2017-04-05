package com.duedil.mesos.java;

import com.duedil.mesos.java.executor.ActionableListener;
import com.duedil.mesos.java.executor.ExecutorConnection;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.FrameworkID;
import org.junit.Test;

import java.net.URISyntaxException;

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
        assertEquals(connection.getMaxRetries(), maxRetries);
    }

}
