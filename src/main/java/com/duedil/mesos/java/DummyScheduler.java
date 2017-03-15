package com.duedil.mesos.java;

import com.google.protobuf.ByteString;

import org.apache.mesos.v1.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;

/**
 * Test scheduler to verify basic Scheduler behaviours
 */
public class DummyScheduler implements Scheduler {

    public static void main(String[] args) {
        DummyScheduler scheduler = new DummyScheduler();

        Protos.FrameworkInfo fi = Protos.FrameworkInfo.newBuilder()
                .setName("TestingTesting123")
                .setUser("root")
                .build();
        URI masterUri = URI.create("http://192.168.33.50:5050");

        MesosSchedulerDriver d = new MesosSchedulerDriver(scheduler, fi, masterUri);
        d.start();
    }

    private static final Logger LOG = LoggerFactory.getLogger(DummyScheduler.class);

    @Override
    public void registered(SchedulerDriver driver, Protos.FrameworkID frameworkId, Protos.MasterInfo masterInfo) {
        LOG.info("REGISTERED");
    }

    @Override
    public void reregistered(SchedulerDriver driver, Protos.MasterInfo masterInfo) {
        LOG.info("REREGISTERED");
    }

    @Override
    public void disconnected(SchedulerDriver driver) {
        LOG.info("DISCONNECTED");
    }

    @Override
    public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
        LOG.info("OFFERED RESOURCE");
    }

    @Override
    public void offerRescinded(SchedulerDriver driver, Protos.OfferID offerId) {
        LOG.info("OFFER RESCINDED");
    }

    @Override
    public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {
        LOG.info("STATUS UPDATE");
    }

    @Override
    public void frameworkMessage(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.AgentID agentId, ByteString message) {
        LOG.info("FRAMEWORK MESSAGE");
    }

    @Override
    public void agentLost(SchedulerDriver driver, Protos.AgentID agentId) {
        LOG.info("AGENT LOST");
    }

    @Override
    public void executorLost(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.AgentID agentId, int status) {
        LOG.info("EXECUTOR LOST");
    }

    @Override
    public void error(SchedulerDriver driver, String message) {
        LOG.info("ERROR");
    }
}
