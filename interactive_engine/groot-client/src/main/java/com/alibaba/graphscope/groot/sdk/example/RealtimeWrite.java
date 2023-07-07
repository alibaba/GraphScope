package com.alibaba.graphscope.groot.sdk.example;

import com.alibaba.graphscope.groot.sdk.GrootClient;

import java.util.HashMap;
import java.util.Map;

public class RealtimeWrite {

    public static void main(String[] args) {
        GrootClient client = GrootClient.newBuilder().addHost("localhost", 55556).build();
        {
            client.initWriteSession();
            for (int i = 0; i < 10; ++i) {
                Map<String, String> properties = new HashMap<>();
                properties.put("id", String.valueOf(i));
                properties.put("name", "person-" + i);
                properties.put("age", String.valueOf(i + 20));
                client.addVertex("person", properties);

                properties.clear();
                properties.put("id", String.valueOf(i));
                properties.put("name", "software-" + i);
                properties.put("lang", String.valueOf(i + 200));
                client.addVertex("software", properties);
            }

            for (int i = 0; i < 10; ++i) {
                Map<String, String> srcPk = new HashMap<>();
                Map<String, String> dstPk = new HashMap<>();
                Map<String, String> properties = new HashMap<>();

                srcPk.put("id", String.valueOf(i));
                dstPk.put("id", String.valueOf(i));
                properties.put("weight", "weight-" + i);
                client.addEdge("created", "person", "software", srcPk, dstPk, properties);
            }
            long snapshotId = client.commit();
            client.remoteFlush(snapshotId);
            System.out.println("Finished adding vertices and edges");
        }
        {
            client.initWriteSession();
            Map<String, String> srcPk = new HashMap<>();
            Map<String, String> dstPk = new HashMap<>();
            Map<String, String> properties = new HashMap<>();

            srcPk.put("id", String.valueOf(0));
            dstPk.put("id", String.valueOf(0));
            properties.put("weight", "weight-0-updated");
            client.updateEdge("created", "person", "software", srcPk, dstPk, properties);
            long snapshotId = client.commit();
            client.remoteFlush(snapshotId);
            System.out.println("Finished update edge person-0 -> software-0");

            client.initWriteSession();
            client.deleteEdge("created", "person", "software", srcPk, dstPk);
            snapshotId = client.commit();
            client.remoteFlush(snapshotId);
            System.out.println("Finished delete edge person-0 -> software-0");
        }
        {
            client.initWriteSession();
            Map<String, String> properties = new HashMap<>();
            properties.put("id", String.valueOf(0));
            properties.put("name", "marko-0-updated");
            client.updateVertex("person", properties);
            long snapshotId = client.commit();
            client.remoteFlush(snapshotId);
            System.out.println("Finished update vertex person-0");

            client.initWriteSession();
            Map<String, String> pk_properties = new HashMap<>();
            client.deleteVertex("person", pk_properties);
            snapshotId = client.commit();
            client.remoteFlush(snapshotId);
            System.out.println("Finished delete vertex person-0");
        }
    }
}
