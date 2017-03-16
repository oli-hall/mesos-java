package com.duedil.mesos.java;

import com.google.api.client.util.Lists;
import com.google.protobuf.ByteString;

import org.apache.mesos.v1.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.logging.LogManager;

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

        double offerCpu = 0.0;
        double offerMem = 0.0;
        String offerRole = "";
        Protos.AgentID agentId = null;
        List<Protos.OfferID> offerIds = Lists.newArrayList();
        for (Protos.Offer offer : offers) {

            offerIds.add(offer.getId());
            agentId = offer.getAgentId();

            for (Protos.Resource resource : offer.getResourcesList()) {
                offerRole = resource.getRole();
                if ("cpus".equals(resource.getName())) {
                    offerCpu = resource.getScalar().getValue();
                } else if ("mem".equals(resource.getName())) {
                    offerMem = resource.getScalar().getValue();
                }
            }

            LOG.debug(String.format("Received offer for CPUs: %f, Mem: %f, Role: %s", offerCpu, offerMem, offerRole));
            System.out.println(String.format("Received offer for CPUs: %f, Mem: %f, Role: %s", offerCpu, offerMem, offerRole));
        }

        Protos.TaskID taskId = Protos.TaskID.newBuilder()
                .setValue(UUID.randomUUID().toString())
                .build();
        Protos.TaskInfo taskInfo = Protos.TaskInfo.newBuilder()
                .setName("TestyMcTestFace")
                .setTaskId(taskId)
                .setAgentId(agentId)
                .setExecutor(
                        Protos.ExecutorInfo.newBuilder()
                                .setExecutorId(
                                        Protos.ExecutorID.newBuilder()
                                                .setValue(taskId.getValue())
                                                .build()
                                )
                                .build()
                ) // TODO a command etc
                .addResources(
                        Protos.Resource.newBuilder()
                                .setName("cpus")
                                .setType(Protos.Value.Type.SCALAR)
                                .setRole(offerRole)
                                .setScalar(
                                        Protos.Value.Scalar.newBuilder()
                                                .setValue(offerCpu)
                                                .build()
                                )
                )
                .addResources(
                        Protos.Resource.newBuilder()
                                .setName("mem")
                                .setType(Protos.Value.Type.SCALAR)
                                .setRole(offerRole)
                                .setScalar(
                                        Protos.Value.Scalar.newBuilder()
                                                .setValue(offerMem)
                                                .build()
                                )
                )
                .build();

        List<Protos.TaskInfo> tasks = Lists.newArrayList();
        tasks.add(taskInfo);


        System.out.println(String.format("Launching tasks %s for offer IDs %s", tasks.toString(), offerIds.toString()));
        LOG.debug(String.format("Launching tasks %s for offer IDs %s", tasks.toString(), offerIds.toString()));
        driver.launchTasks(offerIds, tasks, null);
    }

    @Override
    public void offerRescinded(SchedulerDriver driver, Protos.OfferID offerId) {
        LOG.info("OFFER RESCINDED");
    }

    @Override
    public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {
        LOG.info("Task update: " + status.getState().toString());
        switch (status.getState()) {
            case TASK_FAILED:
                LOG.error("Task failed: " + status.getMessage());
        }

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
