package com.alibaba.maxgraph.dataload;

import com.alibaba.graphscope.groot.sdk.Client;

import java.io.IOException;

public class IngestDataCommand extends DataCommand {

    public IngestDataCommand(String dataPath) throws IOException {
        super(dataPath);
    }

    public void run() {
        Client client = new Client(graphEndpoint, "");
        client.ingestData(dataPath);
    }
}
