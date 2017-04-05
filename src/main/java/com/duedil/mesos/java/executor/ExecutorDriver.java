package com.duedil.mesos.java.executor;

import org.apache.mesos.v1.Protos.Status;

/**
 * Interface for Mesos executor drivers. Users may wish to extend this class in mock 
 * objects for tests.
 */
public interface ExecutorDriver {
    
    /**
     * Starts the executor driver. This needs to be called before any other driver calls
     * are made.
     */
    void start();

    /**
     * Stops the executor driver.
     */
    void stop();

    /**
     * Aborts the driver so that no more callbacks can be made to the executor.  The semantics
     * of abort and stop have deliberately been separated so that code can detect an aborted
     * driver (i.e., via the return status of ExecutorDriver.join), and instantiate and start
     * another driver if desired (from within the same process, although this functionality
     * is currently not supported for executors).
     */
    void abort();

    /**
     * Waits for the driver to be stopped or aborted, possibly blocking the current thread
     * indefinitely.  The return status of this function can be used to determine if the driver
     * was aborted (see mesos.proto for a description of Status).
     */
    void join();

    /**
     * Starts and immediately joins (i.e., blocks on) the driver.
     */
    void run();

    /**
     * Sends a status update to the framework scheduler, retrying as necessary until an
     * acknowledgement has been received or the executor is terminated (in which case, a
     * TASK_LOST status update will be sent). See Scheduler.statusUpdate for more information
     * about status update acknowledgements.
     */
    void sendStatusUpdate(Status status);

    /**
     * Sends a message to the framework scheduler. These messages are best effort; do not
     * expect a framework message to be retransmitted in any reliable fashion.
     */
    void sendFrameworkMessage(byte[] data);
}
