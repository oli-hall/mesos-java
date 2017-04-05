package com.duedil.mesos.java;

import java.net.URI;

public class Utils {

    public static URI schedulerEndpoint(URI masterUri) {
        return URI.create(masterUri.toString() + "/api/v1/scheduler");
    }

    public static URI executorEndpoint() {
        return URI.create("http://127.0.0.1:5050/api/v1/executor");
    }

    public static URI versionEndpoint(URI masterUri) {
        return URI.create(masterUri.toString() + "/version");
    }
}
