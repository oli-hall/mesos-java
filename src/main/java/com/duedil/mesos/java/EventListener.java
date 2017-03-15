package com.duedil.mesos.java;

import org.apache.mesos.v1.scheduler.Protos.Event;

public interface EventListener {

    /**
     * Called when a Scheduler Event is received.
     * @param event The Protobuf object representing the event received.
     */
    void onEvent(Event event);
}
