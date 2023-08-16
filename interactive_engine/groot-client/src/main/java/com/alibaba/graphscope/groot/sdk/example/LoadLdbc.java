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
package com.alibaba.graphscope.groot.sdk.example;

import com.alibaba.graphscope.groot.sdk.GrootClient;
import com.alibaba.graphscope.groot.sdk.schema.Edge;
import com.alibaba.graphscope.groot.sdk.schema.Vertex;

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
    public static void main(String[] args) throws IOException, ParseException {
        String dataDir = args[0];
        String host = args[1];
        int port = Integer.valueOf(args[2]);
        int batchSize = Integer.valueOf(args[3]);
        System.out.println(
                "dataDir ["
                        + dataDir
                        + "] host ["
                        + host
                        + "] port ["
                        + port
                        + "] batch ["
                        + batchSize
                        + "]");

        GrootClient client = GrootClient.newBuilder().addHost(host, port).build();
        int processed = 0;
        int ignored = 0;
        for (Path path : Files.list(Paths.get(dataDir)).collect(Collectors.toList())) {
            System.out.println("process file [" + path.getFileName() + "]");
            String fileName = path.getFileName().toString();
            if (!fileName.endsWith("_0_0.csv")
                    || fileName.startsWith(".")
                    || fileName.startsWith("person_speaks_language")
                    || fileName.startsWith("person_email_emailaddress")) {
                System.out.println("ignore [" + fileName + "]");
                ignored++;
                continue;
            }
            String name = fileName.substring(0, fileName.length() - 8);
            String[] items = name.split("_");
            if (items.length == 1) {
                String label = capitalize(items[0]);
                System.out.println("vertex table: [" + label + "]");
                processVertex(client, label, path, batchSize);
            } else {
                String srcLabel = capitalize(items[0]);
                String label = items[1];
                String dstLabel = capitalize(items[2]);
                System.out.println(
                        "edge table: [" + srcLabel + "-" + label + "->" + dstLabel + "]");
                processEdge(client, label, srcLabel, dstLabel, path, batchSize);
            }
            processed++;
        }
        System.out.println(
                "Total ["
                        + (processed + ignored)
                        + "]. processed ["
                        + processed
                        + "], ignored ["
                        + ignored
                        + "]");
    }

    public static String capitalize(String origin) {
        return origin;
    }

    private static void processVertex(GrootClient client, String label, Path path, int batchSize)
            throws IOException {
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
                    List<Vertex> vertices = new ArrayList<>();
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
                    vertices.add(new Vertex(label, properties));
                    count++;
                    if (count == batchSize) {
                        try {
                            snapshotId = client.addVertices(vertices);
                        } catch (Exception e) {
                            System.err.println(
                                    "add vertex label ["
                                            + label
                                            + "], properties ["
                                            + properties
                                            + "] failed. Reason: "
                                            + e);
                        }
                        count = 0;
                    }
                }
            }
        }
        System.out.println("flush snapshotId [" + snapshotId + "]");
        client.remoteFlush(snapshotId);
        System.out.println("done");
    }

    private static void processEdge(
            GrootClient client,
            String label,
            String srcLabel,
            String dstLabel,
            Path path,
            int batchSize)
            throws IOException {
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
                    List<Edge> edges = new ArrayList<>();
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
                    edges.add(
                            new Edge(
                                    label,
                                    srcLabel,
                                    dstLabel,
                                    Collections.singletonMap("id", items[0]),
                                    Collections.singletonMap("id", items[1]),
                                    properties));
                    count++;
                    if (count == batchSize) {
                        snapshotId = client.addEdges(edges);
                        count = 0;
                    }
                }
            }
        }
        System.out.println("flush snapshotId [" + snapshotId + "]");
        client.remoteFlush(snapshotId);
        System.out.println("done");
    }
}
