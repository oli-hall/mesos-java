package com.duedil.mesos.java.executor;

import org.apache.mesos.v1.executor.Protos.Event;

public interface ActionableListener {

    /**
     * Called when an Executor Event is received.
     * @param event The Protobuf object representing the event received.
     */
    void onEvent(Event event);

}
