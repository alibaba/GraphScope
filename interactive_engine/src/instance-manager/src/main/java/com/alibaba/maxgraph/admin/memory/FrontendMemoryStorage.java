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
package com.alibaba.maxgraph.admin.memory;

import com.google.common.collect.Maps;

import java.util.Map;

public class FrontendMemoryStorage {
    private static FrontendMemoryStorage frontendStorage = new FrontendMemoryStorage();

    private Map<String, InstanceEntity> graphFrontendEndpoint = Maps.newConcurrentMap();

    private FrontendMemoryStorage() {

    }

    public static FrontendMemoryStorage getFrontendStorage() {
        return frontendStorage;
    }

    public void addFrontendEndpoint(String graphName, InstanceEntity instanceEntity) {
        if (this.graphFrontendEndpoint.containsKey(graphName)) {
            InstanceEntity existEndpoint = this.graphFrontendEndpoint.get(graphName);
            throw new RuntimeException("interactive endpoint[" + existEndpoint.getFrontEndpoint() + "] already exist in graph " + graphName);
        }
        this.graphFrontendEndpoint.put(graphName, instanceEntity);
    }

    public void removeFrontendEndpoint(String graphName) {
        this.graphFrontendEndpoint.remove(graphName);
    }

    public InstanceEntity getFrontendEndpoint(String graphName) {
        return this.graphFrontendEndpoint.get(graphName);
    }

    public Map<String, InstanceEntity> getGraphFrontendEndpoint() {
        return this.graphFrontendEndpoint;
    }
}
