package com.alibaba.maxgraph.dataload;

import com.alibaba.graphscope.groot.sdk.MaxGraphClient;

import java.io.IOException;

public class IngestDataCommand extends DataCommand {

    public IngestDataCommand(String dataPath) throws IOException {
        super(dataPath);
    }

    public void run() {
        MaxGraphClient client = MaxGraphClient.newBuilder().setHosts(graphEndpoint).build();
        client.ingestData(dataPath);
    }
}
