package com.alibaba.graphscope.gaia.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.alibaba.graphscope.gaia.clients.GraphClient;
import com.alibaba.graphscope.gaia.clients.GraphSystem;
import com.alibaba.graphscope.gaia.clients.cypher.CypherGraphClient;
import com.alibaba.graphscope.gaia.clients.gremlin.GremlinGraphClient;
import com.alibaba.graphscope.gaia.clients.kuzu.KuzuGraphClient;
import com.alibaba.graphscope.gaia.common.Configuration;

public class BenchmarkSystemUtil {
    public static List<GraphSystem> initSystems(Configuration configuration) throws Exception {
        String username = configuration.getString(Configuration.AUTH_USERNAME, "");
        String password = configuration.getString(Configuration.AUTH_PASSWORD, "");
        List<GraphSystem> systemClients = new ArrayList<>();
        int systemCount = 1;
        while (true) {
            Optional<String> systemName = configuration.getOption("system." + systemCount + ".name");
            if (!systemName.isPresent()) {
                break;
            }
            Optional<String> endpoint = configuration.getOption("system." + systemCount + ".endpoint");
            Optional<String> path = configuration.getOption("system." + systemCount + ".path");
            String clientOpt = configuration.getString("system." + systemCount + ".client.opt");
            GraphClient client;
            switch(clientOpt.toLowerCase()) {
                case "gremlin":
                    if (endpoint.isPresent()) {
                        client = new GremlinGraphClient(endpoint.get(), username, password);
                    } else {
                        throw new IllegalArgumentException("Gremlin client must have endpoint of the database service");
                    }
                    break;
                case "cypher":
                    if (endpoint.isPresent()) {
                        client = new CypherGraphClient(endpoint.get(), username, password);
                    } else {
                        throw new IllegalArgumentException("Cypher client must have endpoint of the database service");
                    }
                    break;
                case "kuzu":
                    if (path.isPresent()) {
                        client = new KuzuGraphClient(path.get());
                    } else {
                        throw new IllegalArgumentException("Kuzu client must have path to access the database");
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported client type: " + clientOpt);
            }
            systemClients.add(new GraphSystem(systemName.get(), client));
            systemCount++;
        }
        return systemClients;
    }
}
