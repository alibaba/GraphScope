/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.frontendservice.server;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0;

import java.util.HashMap;
import java.util.Map;

public class GremlinClientMain {
    public static void main(String[] args) {
        Cluster cluster = null;
        Client client = null;

        try {
            cluster = getCluster("localhost", 50330);
            client = cluster.connect();
            ResultSet resultSet = client.submit("g.V(933).out().out()");
            System.out.println(resultSet);
            StringBuilder sb = new StringBuilder();
            resultSet.stream().forEach((result) -> {
                sb.append("==>");
                sb.append(result.getString());
                sb.append("\n");
            });
            System.out.println(sb.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static Cluster getCluster(String ip, Integer port) {
        MessageSerializer serializer = new GryoMessageSerializerV1d0();
        Map<String, Object> config = new HashMap<String, Object>() {
            {
                this.put("serializeResultToString", true);
            }
        };
        Cluster.Builder builder = Cluster.build();
        builder.addContactPoint(ip);
        builder.port(port.intValue());
        serializer.configure(config, (Map)null);
        builder.serializer(serializer);
        return builder.create();
    }
}
