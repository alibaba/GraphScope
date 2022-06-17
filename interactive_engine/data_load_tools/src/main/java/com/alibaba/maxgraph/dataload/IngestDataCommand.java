package com.alibaba.maxgraph.dataload;

import com.alibaba.graphscope.groot.sdk.MaxGraphClient;

import java.io.IOException;

public class IngestDataCommand extends DataCommand {

    public IngestDataCommand(String dataPath, boolean isFromOSS, String uniquePath)
            throws IOException {
        super(dataPath, isFromOSS, uniquePath);
    }

    public void run() {
        MaxGraphClient client =
                MaxGraphClient.newBuilder()
                        .setHosts(graphEndpoint)
                        .setUsername(username)
                        .setPassword(password)
                        .build();
        // dataPath = Paths.get(dataPath, uniquePath).toString();
        dataPath = dataPath + "/" + uniquePath;
        client.ingestData(dataPath);
    }
}
