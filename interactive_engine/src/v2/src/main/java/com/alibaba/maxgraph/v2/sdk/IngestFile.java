package com.alibaba.maxgraph.v2.sdk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IngestFile {
    private static final Logger logger = LoggerFactory.getLogger(IngestFile.class);

    public static void main(String[] args) throws IOException {
        String inputFile = args[0];
        String host = args[1];
        int port = Integer.valueOf(args[2]);
        int batchSize = Integer.valueOf(args[3]);

        Client client = new Client(host, port);
        File file = new File(inputFile);
        String fileName = file.getName();
        String label = fileName.split("_")[0];
        logger.info("file [" + inputFile + "] host [" + host + "] port [" + port + "] label [" + label + "]");
        List<String> propertyNames = new ArrayList<>();
        int count = 0;
        long snapshotId = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (propertyNames.size() == 0) { // First line
                    for (String item : line.split("\\|")) {
                        propertyNames.add(item);
                    }
                } else {
                    Map<String, String> properties = new HashMap<>();
                    String[] items = line.split("\\|");
                    for (int i = 0; i < items.length; i++) {
                        properties.put(propertyNames.get(i), items[i]);
                    }
                    client.addVertex(label, properties);
                    count++;
                    if (count == batchSize) {
                        snapshotId = client.commit();
                        count = 0;
                    }
                }
            }
        }
        long maybeSnapshotId = client.commit();
        long flushSnapshotId = maybeSnapshotId == 0 ? snapshotId : maybeSnapshotId;
        logger.info("flush snapshotId [" + flushSnapshotId + "]");
        client.remoteFlush(flushSnapshotId);
        logger.info("done");
    }
}
