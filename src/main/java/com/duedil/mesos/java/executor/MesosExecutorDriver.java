package com.duedil.mesos.java.executor;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import org.apache.mesos.v1.Protos.ExecutorInfo;
import org.apache.mesos.v1.Protos.FrameworkID;
import org.apache.mesos.v1.Protos.FrameworkInfo;
import org.apache.mesos.v1.Protos.Status;
import org.apache.mesos.v1.executor.Protos.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.api.client.util.Preconditions.checkNotNull;

public class MesosExecutorDriver implements ExecutorDriver, ActionableListener {

    private static final Logger LOG = LoggerFactory.getLogger(MesosExecutorDriver.class);
    private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();

    private final Executor executor;
    private final FrameworkInfo framework;
    private final ExecutorInfo executorInfo;
    private FrameworkID frameworkId;
    private ExecutorConnection conn;

    public MesosExecutorDriver(Executor executor, FrameworkInfo framework, ExecutorInfo executorInfo) {
        this.executor = checkNotNull(executor);
        this.framework = checkNotNull(framework);
        this.frameworkId = framework.getId();
        this.executorInfo = checkNotNull(executorInfo);
        this.conn = null;
    }

    @Override
    public void start() {
        conn = new ExecutorConnection(framework, frameworkId, executorInfo, this);
    }

    @Override
    public void stop() {
        // TODO: kill all the things
    }

    @Override
    public void abort() {

    }

    @Override
    public void join() {
        try {
            conn.join();
        }
        catch (InterruptedException e) {
            LOG.error("Exception thrown waiting for connection join: {}", e.getMessage());
        }
    }

    @Override
    public void run() {
        start();
        conn.run();
        join();
    }

    @Override
    public void sendStatusUpdate(Status status) {

    }

    @Override
    public void sendFrameworkMessage(byte[] data) {

    }

    @Override
    public void onEvent(Event event) {
        LOG.debug("Event: {}", event.getMessage());
    }
}
