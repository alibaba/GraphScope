package com.alibaba.maxgraph.dataload;

import com.alibaba.graphscope.groot.sdk.MaxGraphClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;

public class IngestDataCommand extends DataCommand {
    private static final Logger logger = LoggerFactory.getLogger(IngestDataCommand.class);

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
        configPath = configPath + "/" + uniquePath;
        if (ossAccessID == null || ossAccessKey == null) {
            logger.warn("ossAccessID or ossAccessKey is null, using default configuration.");
            client.ingestData(configPath);
        } else {
            HashMap<String, String> configs = new HashMap<>();
            configs.put("ossAccessID", ossAccessID);
            configs.put("ossAccessKey", ossAccessKey);
            client.ingestData(configPath, configs);
        }
    }
}
