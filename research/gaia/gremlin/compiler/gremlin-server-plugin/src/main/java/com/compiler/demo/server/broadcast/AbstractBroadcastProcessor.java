/*
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

import com.alibaba.pegasus.service.protocol.PegasusClient;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.server.Context;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractBroadcastProcessor implements AutoCloseable {
    protected List<Pair<String, Integer>> hostAddresses = new ArrayList<>();

    public abstract void broadcast(PegasusClient.JobRequest request, Context writeResult);

    public AbstractBroadcastProcessor(List<String> hostInfo) {
        hostInfo.forEach(s -> {
            String[] host = s.split(":");
            hostAddresses.add(Pair.of(host[0], Integer.valueOf(host[1])));
        });
    }
}
