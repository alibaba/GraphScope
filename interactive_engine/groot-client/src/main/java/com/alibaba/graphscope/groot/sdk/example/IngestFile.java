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
import com.alibaba.graphscope.groot.sdk.schema.Vertex;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IngestFile {
    public static void main(String[] args) throws IOException {
        String inputFile = args[0];
        String host = args[1];
        int port = Integer.valueOf(args[2]);
        int batchSize = Integer.valueOf(args[3]);

        GrootClient client = GrootClient.newBuilder().addHost(host, port).build();
        File file = new File(inputFile);
        String fileName = file.getName();
        String label = fileName.split("_")[0];
        System.out.println(
                "file ["
                        + inputFile
                        + "] host ["
                        + host
                        + "] port ["
                        + port
                        + "] label ["
                        + label
                        + "]");
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
                    List<Vertex> vertices = new ArrayList<>();
                    Map<String, String> properties = new HashMap<>();
                    String[] items = line.split("\\|");
                    for (int i = 0; i < items.length; i++) {
                        properties.put(propertyNames.get(i), items[i]);
                    }
                    vertices.add(new Vertex(label, properties));
                    count++;
                    if (count == batchSize) {
                        snapshotId = client.addVertices(vertices);
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
