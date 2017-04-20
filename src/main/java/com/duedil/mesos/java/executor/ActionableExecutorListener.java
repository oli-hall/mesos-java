package com.duedil.mesos.java.executor;

import org.apache.mesos.v1.Protos.TaskInfo;
import org.apache.mesos.v1.executor.Protos.Call.Update;
import org.apache.mesos.v1.executor.Protos.Event;

import java.net.URI;
import java.util.Set;

public interface ActionableExecutorListener {

    /**
     * Called when an Executor Event is received.
     * @param event The Protobuf object representing the event received.
     */
    void onEvent(Event event);

    Set<TaskInfo> getUnacknowledgedTasks();

    Set<Update> getUnacknowledgedUpdates();

    URI getAgentEndpoint();

}
