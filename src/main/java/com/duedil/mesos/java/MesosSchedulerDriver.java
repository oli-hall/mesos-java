package com.duedil.mesos.java;

import org.apache.mesos.v1.Protos;

import java.util.List;

public class MesosSchedulerDriver implements SchedulerDriver {

    @Override
    public void start() {

    }

    @Override
    public void stop(boolean failover) {

    }

    @Override
    public void abort() {

    }

    @Override
    public void join() {

    }

    @Override
    public void run() {

    }

    @Override
    public void requestResources(List<Protos.Request> requests) {

    }

    @Override
    public void launchTasks(List<Protos.OfferID> offerIds, List<Protos.Task> tasks, Protos.Filters filters) {

    }

    @Override
    public void killTask(Protos.TaskID taskId) {

    }

    @Override
    public void acceptOffers(List<Protos.OfferID> offerIds, List<Protos.Offer.Operation> operations, Protos.Filters filters) {

    }

    @Override
    public void declineOffer(Protos.OfferID offerId, Protos.Filters filters) {

    }

    @Override
    public void reviveOffers() {

    }

    @Override
    public void suppressOffers() {

    }

    @Override
    public void acknowledgeStatusUpdate(Protos.Status status) {

    }

    @Override
    public void sendFrameworkMessage(Protos.ExecutorID executorId, Protos.AgentID agentId, byte[] data) {

    }

    @Override
    public void reconcileTasks(List<Protos.Task> tasks) {

    }
}
