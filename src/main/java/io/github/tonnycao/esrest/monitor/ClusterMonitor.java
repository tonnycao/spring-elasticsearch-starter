package io.github.tonnycao.esrest.monitor;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.ClusterClient;
import org.elasticsearch.client.RequestOptions;

import java.io.IOException;

public class ClusterMonitor {

    private final ClusterClient clusterClient;

    public ClusterMonitor(ClusterClient clusterClient) {
        this.clusterClient = clusterClient;
    }

    public ClusterHealthResponse getClusterHealth() throws IOException {
        ClusterHealthRequest request = new ClusterHealthRequest();
        return clusterClient.health(request, RequestOptions.DEFAULT);
    }
}
