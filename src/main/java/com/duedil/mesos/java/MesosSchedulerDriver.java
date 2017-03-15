package com.duedil.mesos.java;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.http.protobuf.ProtoHttpContent;
import com.google.api.client.json.Json;
import com.google.api.client.json.JsonParser;
import com.google.api.client.protobuf.ProtoObjectParser;
import com.google.api.client.util.IOUtils;
import com.google.protobuf.ByteString;

import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Parser;

import org.apache.mesos.v1.Protos;
import org.apache.mesos.v1.Protos.AgentID;
import org.apache.mesos.v1.Protos.ExecutorID;
import org.apache.mesos.v1.Protos.Filters;
import org.apache.mesos.v1.Protos.FrameworkID;
import org.apache.mesos.v1.Protos.FrameworkInfo;
import org.apache.mesos.v1.Protos.MasterInfo;
import org.apache.mesos.v1.Protos.Offer.Operation;
import org.apache.mesos.v1.Protos.Offer.Operation.Launch;
import org.apache.mesos.v1.Protos.OfferID;
import org.apache.mesos.v1.Protos.URL;
import org.apache.mesos.v1.Protos.Request;
import org.apache.mesos.v1.Protos.TaskID;
import org.apache.mesos.v1.Protos.TaskInfo;
import org.apache.mesos.v1.Protos.TaskStatus;
import org.apache.mesos.v1.Protos.VersionInfo;
import org.apache.mesos.v1.scheduler.Protos.Call;
import org.apache.mesos.v1.scheduler.Protos.Call.Subscribe;
import org.apache.mesos.v1.scheduler.Protos.Call.Accept;
import org.apache.mesos.v1.scheduler.Protos.Call.Acknowledge;
import org.apache.mesos.v1.scheduler.Protos.Call.Decline;
import org.apache.mesos.v1.scheduler.Protos.Call.Reconcile;
import org.apache.mesos.v1.scheduler.Protos.Event;
import org.apache.mesos.v1.scheduler.Protos.Event.Error;
import org.apache.mesos.v1.scheduler.Protos.Event.Failure;
import org.apache.mesos.v1.scheduler.Protos.Event.Message;
import org.apache.mesos.v1.scheduler.Protos.Event.Offers;
import org.apache.mesos.v1.scheduler.Protos.Event.Rescind;
import org.apache.mesos.v1.scheduler.Protos.Event.Subscribed;
import org.apache.mesos.v1.scheduler.Protos.Event.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import static com.duedil.mesos.java.Utils.schedulerEndpoint;
import static com.duedil.mesos.java.Utils.versionEndpoint;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.http.HttpStatus.SC_MULTIPLE_CHOICES;
import static org.apache.http.HttpStatus.SC_OK;
import static org.apache.mesos.v1.Protos.Offer.Operation.Type.LAUNCH;
import static org.apache.mesos.v1.scheduler.Protos.Call.Type.ACCEPT;
import static org.apache.mesos.v1.scheduler.Protos.Call.Type.ACKNOWLEDGE;
import static org.apache.mesos.v1.scheduler.Protos.Call.Type.DECLINE;
import static org.apache.mesos.v1.scheduler.Protos.Call.Type.KILL;
import static org.apache.mesos.v1.scheduler.Protos.Call.Type.MESSAGE;
import static org.apache.mesos.v1.scheduler.Protos.Call.Type.RECONCILE;
import static org.apache.mesos.v1.scheduler.Protos.Call.Type.REQUEST;
import static org.apache.mesos.v1.scheduler.Protos.Call.Type.REVIVE;
import static org.apache.mesos.v1.scheduler.Protos.Call.Type.SUPPRESS;
import static org.apache.mesos.v1.scheduler.Protos.Call.Type.TEARDOWN;

public class MesosSchedulerDriver implements SchedulerDriver, EventListener {

    private static final Logger LOG = LoggerFactory.getLogger(MesosSchedulerDriver.class);

    private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();

    private final Scheduler scheduler;
    private final FrameworkInfo framework;
    private final URI masterUri;
    private final boolean implicitAcknowledgements;
    private boolean failover;
    // TODO wrap this into framework var? framework is immutable class
    private FrameworkID frameworkId;
    private String version;

    public MesosSchedulerDriver(Scheduler scheduler, FrameworkInfo framework, URI masterUri) {
        this(scheduler, framework, masterUri, false);
    }

    public MesosSchedulerDriver(Scheduler scheduler, FrameworkInfo framework, URI masterUri,
                                boolean implicitAcknowledgements) {
        this.scheduler = checkNotNull(scheduler);
        this.framework = checkNotNull(framework);
        this.masterUri = checkNotNull(masterUri);
        this.implicitAcknowledgements = checkNotNull(implicitAcknowledgements);
        this.version = null;
        this.failover = false;
    }

    // TODO can this be connected but not have a framework ID? can have framework ID and not be connected

    // TODO behaviour from framework getter

    @Override
    public void start() {
        // superclass start()
        // set uri = master
        // if uri starts with zk:// or zoo://
            // set detector to MasterDetector (MASSIVE TODO), with uri equal to non-scheme suffix of URI
            // start detector
        // else
            // if uri doesn't have a port suffix
                // add :5050
            // changeMaster(uri);
        SchedulerConnection conn = new SchedulerConnection(framework, this, masterUri, frameworkId);
        conn.run();
    }

    public void stop() {
        stop(false);
    }

    @Override
    public void stop(boolean failover) {
        // lock
            // assign failover to class var
            this.failover = failover;
            // grab detector, set class level detector to null

        // if there was a detector, stop it

        // call stop on the Process superclass
    }

    private void changeMaster(URI master) {
        version = getVersion(master);
        // super changeMaster(master)
        close();
    }

    private String getVersion(URI master) {
        if (master != null) {
            HttpRequestFactory requestFactory = HTTP_TRANSPORT.createRequestFactory(
                    new HttpRequestInitializer() {
                        @Override
                        public void initialize(HttpRequest request) {
                            request.setParser(new ProtoObjectParser());
                        }
                    });

            GenericUrl url = new GenericUrl(versionEndpoint(master));
            try {
                HttpRequest request = requestFactory.buildGetRequest(url);
                HttpResponse response = request.execute();
                if (response.getStatusCode() < SC_OK || response.getStatusCode() >= SC_MULTIPLE_CHOICES) {
                    return null;
                }

                VersionInfo versionInfo = response.parseAs(VersionInfo.class);
                return versionInfo.getVersion();
            } catch(IOException e) {
                // TODO log error
                // TODO close connection
                LOG.error("BLEARGH", e.toString());
            }
        }
        return "";
    }

    private void shutDown() {
        if (!failover) {
            tearDown();
        }
    }

    private void close() {
        // TODO close connection
        // is this persistent connection needed in the same manner?
    }

    private void tearDown() {
//        if (connected()) {
            if (frameworkId != null) {
                Call teardown = Call.newBuilder()
                        .setType(TEARDOWN)
                        .setFrameworkId(frameworkId)
                        .build();
                send(teardown);
                // unset ID from framework var if set
            }
//        }
    }

    private void onNewMasterDetectedMessage(ByteString data) {
        URI master = null;
//        // is this a FrameworkInfo? URL?
//        URL url = parseFrom(data); // ????
//        if (url.hasAddress()) {
//            master = String.format("%s:%d", url.getAddress().getIp(), url.getAddress().getPort());
//        }

        if (master != null) {
            changeMaster(master);
        }
    }

    private void onNoMasterDetectedMessage() {
        changeMaster(null);
    }

    private void send(Call body) {
        // TODO headers
        // TODO lock?
        // TODO if not connected, raise exception
        checkNotNull(body);

        // TODO stream ID?
//        if (streamId is set) {
//            // set stream id in headers
//        }

        HttpRequestFactory requestFactory = HTTP_TRANSPORT.createRequestFactory(
                new HttpRequestInitializer() {
                    @Override
                    public void initialize(HttpRequest request) {
                        // TODO ???
                        request.setParser(new ProtoObjectParser());                    }
                });

        GenericUrl url = new GenericUrl(schedulerEndpoint(this.masterUri));
        HttpContent content = new ProtoHttpContent(body);
        try {
            HttpRequest request = requestFactory.buildPostRequest(url, content);
            HttpResponse response = request.execute();
            if (response.getStatusCode() < SC_OK || response.getStatusCode() >= SC_MULTIPLE_CHOICES) {
                throw new RuntimeException(String.format(
                        "Failed to send request (%d): %s\n%s",
                        response.getStatusCode(),
                        response.parseAsString(),
                        body.toString()));
            }
            try (InputStream s = response.getContent()) {
                Scanner sc = new Scanner(s).useDelimiter("\\A");
                String m = sc.hasNext() ? sc.next() : "";
                System.out.println("Received response:" + m);
            }
        } catch (IOException e) {
            // TODO call close
            LOG.error("IOException: ABORT ABORT ABORT");
            throw new RuntimeException(e);
        }
        // TODO read response
        // what type is response? Nothing uses the response anyhow.
        // Convert back to datastructure/JSON, return it?
    }

    @Override
    public void requestResources(List<Request> requests) {
        checkNotNull(frameworkId);
        checkNotNull(requests);

        Call.Request.Builder request = Call.Request.newBuilder()
                .addAllRequests(requests);

        Call resReq = Call.newBuilder()
                .setType(REQUEST)
                .setRequest(request)
                .setFrameworkId(frameworkId)
                .build();
        send(resReq);
    }

    @Override
    public void acceptOffers(List<OfferID> offerIds, List<Operation> operations, Filters filters) {
        if (operations == null || operations.isEmpty()) {
            declineOffers(offerIds, filters);
            return;
        }

        checkNotNull(frameworkId);
        checkNotNull(offerIds);

        Accept.Builder accept = Accept.newBuilder()
                .addAllOfferIds(offerIds)
                .addAllOperations(operations);

        if (filters != null) {
            accept.setFilters(filters);
        }

        Call call = Call.newBuilder()
                .setType(ACCEPT)
                .setAccept(accept)
                .setFrameworkId(frameworkId)
                .build();
        send(call);
    }

    @Override
    public void launchTasks(List<OfferID> offerIds, List<TaskInfo> tasks, Filters filters) {
        if (tasks == null || tasks.isEmpty()) {
            declineOffers(offerIds, filters);
            return;
        }
        checkNotNull(frameworkId);
        checkNotNull(offerIds);

        Operation launch = Operation.newBuilder()
                .setType(LAUNCH)
                .setLaunch(Launch.newBuilder()
                        .addAllTaskInfos(tasks)
                )
                .build();

        List<Operation> operations = Arrays.asList(launch);

        acceptOffers(offerIds, operations, filters);
    }

    @Override
    public void declineOffers(List<OfferID> offerIds, Filters filters) {
        checkNotNull(frameworkId);
        checkNotNull(offerIds);

        Decline.Builder decline = Decline.newBuilder()
                .addAllOfferIds(offerIds);

        if (filters != null) {
            decline.setFilters(filters);
        }

        Call call = Call.newBuilder()
                .setType(DECLINE)
                .setDecline(decline)
                .setFrameworkId(frameworkId)
                .build();
        send(call);
    }

    @Override
    public void reviveOffers() {
        // if not connected
        // return

        checkNotNull(frameworkId);

        Call revive = Call.newBuilder()
                .setType(REVIVE)
                .setFrameworkId(frameworkId)
                .build();
        send(revive);
    }

    @Override
    public void suppressOffers() {
        checkNotNull(frameworkId);

        Call suppress = Call.newBuilder()
                .setType(SUPPRESS)
                .setFrameworkId(frameworkId)
                .build();
        send(suppress);
    }

    @Override
    public void killTask(TaskID taskId) {
        checkNotNull(frameworkId);

        Call kill = Call.newBuilder()
                .setType(KILL)
                .setFrameworkId(frameworkId)
                .build();
        send(kill);
    }

    @Override
    public void acknowledgeStatusUpdate(TaskStatus status) {
        checkNotNull(frameworkId);
        checkNotNull(status);

        Acknowledge.Builder acknowledge = Acknowledge.newBuilder()
                .setAgentId(status.getAgentId())
                .setTaskId(status.getTaskId())
                .setUuid(status.getUuid());

        Call call = Call.newBuilder()
                .setType(ACKNOWLEDGE)
                .setAcknowledge(acknowledge)
                .setFrameworkId(frameworkId)
                .build();

        send(call);
    }

    @Override
    public void sendFrameworkMessage(ExecutorID executorId, AgentID agentId, ByteString data) {
        // TODO replace frameworkID validity checks with isConnected() method or similar?
        checkNotNull(frameworkId);
        checkNotNull(executorId);
        checkNotNull(agentId);

        Call.Message.Builder message = Call.Message.newBuilder()
                .setAgentId(agentId)
                .setExecutorId(executorId)
                .setData(data);

        Call call = Call.newBuilder()
                .setType(MESSAGE)
                .setMessage(message)
                .setFrameworkId(frameworkId)
                .build();
        send(call);
    }

    @Override
    public void reconcileTasks(List<TaskInfo> tasks) {
        checkNotNull(frameworkId);

        Reconcile.Builder reconcile = Reconcile.newBuilder();

        Call call = Call.newBuilder()
                .setType(RECONCILE)
                .setReconcile(reconcile)
                .setFrameworkId(frameworkId)
                .build();
        send(call);
    }

    private void onSubscribed(Subscribed subscribed) {
        boolean reregistered = this.frameworkId != null;
        frameworkId = subscribed.getFrameworkId();
        // TODO keep master as URI throughout?
        // if version is set, add version to master info

        if (reregistered) {
            scheduler.reregistered(this, subscribed.getMasterInfo());
        } else {
            scheduler.registered(this, frameworkId, subscribed.getMasterInfo());
        }
    }

    // TODO what calls this?
    private void onClose() {
        close();
        scheduler.disconnected(this);
    }

    private void onOffers(Offers offers) {
        scheduler.resourceOffers(this, offers.getOffersList());
    }

    private void onRescind(Rescind rescind) {
        scheduler.offerRescinded(this, rescind.getOfferId());
    }

    private void onUpdate(Update update) {
        scheduler.statusUpdate(this, update.getStatus());
        if (implicitAcknowledgements) {
            acknowledgeStatusUpdate(update.getStatus());
        }
    }

    private void onMessage(Message message) {
        scheduler.frameworkMessage(
                this,
                message.getExecutorId(),
                message.getAgentId(),
                message.getData());
    }

    private void onFailure(Failure failure) {
        if (failure.hasExecutorId()) {
            scheduler.executorLost(
                    this,
                    failure.getExecutorId(),
                    failure.getAgentId(),
                    failure.getStatus());
        } else {
            scheduler.agentLost(this, failure.getAgentId());
        }
    }

    private void onError(Error error) {
        scheduler.error(this, error.getMessage());
    }

    @Override
    public void onEvent(Event event) {
        switch (event.getType()) {
            case SUBSCRIBED:
                onSubscribed(event.getSubscribed());
                break;
            case OFFERS:
                onOffers(event.getOffers());
                break;
            case RESCIND:
                onRescind(event.getRescind());
                break;
            case UPDATE:
                onUpdate(event.getUpdate());
                break;
            case MESSAGE:
                onMessage(event.getMessage());
                break;
            case FAILURE:
                onFailure(event.getFailure());
                break;
            case ERROR:
                onError(event.getError());
                break;
            case HEARTBEAT:
                LOG.debug(String.format("Heartbeat: %s", event.toString()));
                break;
            case INVERSE_OFFERS:
                LOG.error(String.format("Inverse Offer: %s", event.toString()));
                break;
            case RESCIND_INVERSE_OFFER:
                LOG.error(String.format("Rescind Inverse Offer: %s", event.toString()));
                break;
            case UNKNOWN:
            default:
                LOG.error(String.format("Unknown event: %s", event.toString()));
        }
    }
}
