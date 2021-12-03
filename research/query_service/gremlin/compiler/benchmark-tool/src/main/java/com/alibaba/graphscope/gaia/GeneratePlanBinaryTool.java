package com.alibaba.graphscope.gaia;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.tinkerpop.gremlin.driver.*;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.ser.GraphBinaryMessageSerializerV1;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

public class GeneratePlanBinaryTool {
    public static void main(String[] args) throws Exception {
        Cluster cluster = null;
        Client client = null;
        try {
            if (args.length < 6) {
                System.err.println("args: query_dir query_pattern binary_dir binary_pattern ip port");
                System.exit(-1);
            }
            File queryDir = new File(args[0]);
            String queryPattern = args[1];
            File binaryDir = new File(args[2]);
            String binaryPattern = args[3];
            String host = args[4];
            int port = Integer.valueOf(args[5]);
            if (!queryDir.exists() || !queryDir.isDirectory()) {
                System.err.println("check directory " + queryDir.getAbsolutePath());
                System.exit(-1);
            }
            if (!binaryDir.exists() || !binaryDir.isDirectory()) {
                System.err.println("check file " + binaryDir.getAbsolutePath());
                System.exit(-1);
            }

            MessageSerializer serializer = new GraphBinaryMessageSerializerV1();
            cluster = Cluster.build()
                    .addContactPoint(host)
                    .port(port)
                    .credentials("admin", "admin")
                    .serializer(serializer)
                    .create();
            client = cluster.connect();

            for (File queryF : queryDir.listFiles()) {
                if (queryF.getName().endsWith(queryPattern)) {
                    String outputFullPath = String.format("%s/%s%s", binaryDir.getAbsolutePath(), FilenameUtils.removeExtension(queryF.getName()), binaryPattern);
                    String query = FileUtils.readFileToString(queryF, StandardCharsets.UTF_8);
                    FileUtils.writeStringToFile(new File(outputFullPath), generateBinary(client, query), StandardCharsets.ISO_8859_1);
                }
            }
        } finally {
            if (client != null) client.close();
            if (cluster != null) cluster.close();
        }
    }

    public static String generateBinary(Client client, String query) throws Exception {
        RequestMessage request = RequestMessage
                .build(Tokens.OPS_EVAL)
                .add(Tokens.ARGS_GREMLIN, query).processor("plan").create();
        CompletableFuture<ResultSet> results = client.submitAsync(request);
        while (!results.isDone()) {
            Thread.sleep(100);
        }
        return results.get().one().getString();
    }
}
