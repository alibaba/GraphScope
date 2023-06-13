package com.alibaba.graphscope.groot.dataload;

import com.alibaba.graphscope.groot.sdk.GrootClient;

import java.io.IOException;

public class IngestDataCommand extends DataCommand {
    public IngestDataCommand(String dataPath) throws IOException {
        super(dataPath);
    }

    public void run() {
        GrootClient client =
                GrootClient.newBuilder()
                        .setHosts(graphEndpoint)
                        .setUsername(username)
                        .setPassword(password)
                        .build();
        System.out.println("Ingesting data with config:");
        ingestConfig.forEach((key, value) -> System.out.println(key + "=" + value));
        System.out.println("Data root path: " + dataRootPath);
        client.ingestData(dataRootPath, ingestConfig);
        System.out.println("Ingest complete.");
    }
}
