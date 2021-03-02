/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.compiler.demo.server.broadcast;

import com.alibaba.pegasus.service.proto.PegasusClient;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.server.Context;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public abstract class BroadcastProcessor {
    protected List<Pair<String, Integer>> hostAddresses = new ArrayList<>();

    public abstract void broadcast(PegasusClient.JobRequest request, Context writeResult);

    public BroadcastProcessor(String hostFile) {
        initFromConfig(hostFile);
    }

    protected void initFromConfig(String hostFile) {
        try {
            File hostF = new File(hostFile);
            if (!hostF.exists() || !hostF.isFile()) {
                return;
            }
            List<String> hostInfo = FileUtils.readLines(hostF, StandardCharsets.UTF_8);
            hostInfo.forEach(s -> {
                String[] host = s.split(":");
                hostAddresses.add(Pair.of(host[0], Integer.valueOf(host[1])));
            });
        } catch (IOException e) {
            throw new RuntimeException("int from config exception " + e);
        }
    }
}
