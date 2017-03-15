package com.duedil.mesos.java;

import java.net.URI;

public class Utils {

    public static URI schedulerEndpoint(URI masterUri) {
        return URI.create(masterUri.toString() + "/v1/api/scheduler");
    }

    public static URI versionEndpoint(URI masterUri) {
        return URI.create(masterUri.toString() + "/version");
    }
}
