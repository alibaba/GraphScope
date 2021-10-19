/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.sdk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LoadLdbc {
    private static final Logger logger = LoggerFactory.getLogger(LoadLdbc.class);

    public static void main(String[] args) throws IOException, ParseException {
        String dataDir = args[0];
        String host = args[1];
        int port = Integer.valueOf(args[2]);
        int batchSize = Integer.valueOf(args[3]);
        logger.info(
                "dataDir ["
                        + dataDir
                        + "] host ["
                        + host
                        + "] port ["
                        + port
                        + "] batch ["
                        + batchSize
                        + "]");

        Client client = new Client(host, port);
        int processed = 0;
        int ignored = 0;
        for (Path path : Files.list(Paths.get(dataDir)).collect(Collectors.toList())) {
            logger.info("process file [" + path.getFileName() + "]");
            String fileName = path.getFileName().toString();
            if (!fileName.endsWith("_0_0.csv")
                    || fileName.startsWith(".")
                    || fileName.startsWith("person_speaks_language")
                    || fileName.startsWith("person_email_emailaddress")) {
                logger.info("ignore [" + fileName + "]");
                ignored++;
                continue;
            }
            String name = fileName.substring(0, fileName.length() - 8);
            String[] items = name.split("_");
            if (items.length == 1) {
                String label = firstUpperCase(items[0]);
                logger.info("vertex table: [" + label + "]");
                processVertex(client, label, path, batchSize);
            } else {
                String srcLabel = firstUpperCase(items[0]);
                String label = items[1];
                String dstLabel = firstUpperCase(items[2]);
                logger.info("edge table: [" + srcLabel + "-" + label + "->" + dstLabel + "]");
                processEdge(client, label, srcLabel, dstLabel, path, batchSize);
            }
            processed++;
        }
        logger.info(
                "Total ["
                        + (processed + ignored)
                        + "]. processed ["
                        + processed
                        + "], ignored ["
                        + ignored
                        + "]");
    }

    public static String firstUpperCase(String origin) {
        return origin;
    }

    private static void processVertex(Client client, String label, Path path, int batchSize)
            throws IOException, ParseException {
        List<String> propertyNames = new ArrayList<>();
        int count = 0;
        long snapshotId = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(path.toFile()))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (propertyNames.size() == 0) { // First line
                    for (String item : line.split("\\|")) {
                        propertyNames.add(item.split(":")[0]);
                    }
                } else {
                    Map<String, String> properties = new HashMap<>();
                    String[] items = line.split("\\|");
                    for (int i = 0; i < items.length; i++) {
                        String propertyName = propertyNames.get(i);
                        String propertyVal = items[i];
                        //                        if (propertyName.endsWith("Date")) {
                        //                            propertyVal =
                        // String.valueOf(df.parse(propertyVal).getTime());
                        //                        } else if (propertyName.equals("birthday")) {
                        //                            propertyVal =
                        // String.valueOf(df2.parse(propertyVal).getTime());
                        //                        }
                        properties.put(propertyName, propertyVal);
                    }
                    try {
                        client.addVertex(label, properties);
                    } catch (Exception e) {
                        logger.error(
                                "add vertex label ["
                                        + label
                                        + "], properties ["
                                        + properties
                                        + "] failed",
                                e);
                    }
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

    private static void processEdge(
            Client client, String label, String srcLabel, String dstLabel, Path path, int batchSize)
            throws IOException, ParseException {
        List<String> propertyNames = new ArrayList<>();
        int count = 0;
        long snapshotId = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(path.toFile()))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (propertyNames.size() == 0) { // First line
                    for (String item : line.split("\\|")) {
                        propertyNames.add(item.split(":")[0]);
                    }
                } else {
                    Map<String, String> properties = new HashMap<>();
                    String[] items = line.split("\\|");
                    for (int i = 2; i < items.length; i++) {
                        String propertyName = propertyNames.get(i);
                        String propertyVal = items[i];
                        //                        if (propertyName.endsWith("Date") &&
                        // propertyVal.indexOf('-') != -1) {
                        //                            try {
                        //                                propertyVal =
                        // String.valueOf(df.parse(propertyVal).getTime());
                        //                            } catch (Exception e) {
                        //                                logger.error("parse failed [" +
                        // propertyVal + "]");
                        //                                throw e;
                        //                            }
                        //                        }
                        properties.put(propertyName, propertyVal);
                    }
                    client.addEdge(
                            label,
                            srcLabel,
                            dstLabel,
                            Collections.singletonMap("id", items[0]),
                            Collections.singletonMap("id", items[1]),
                            properties);
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
