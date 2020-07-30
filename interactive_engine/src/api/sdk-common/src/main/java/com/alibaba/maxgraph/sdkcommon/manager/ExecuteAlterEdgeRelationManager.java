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
package com.alibaba.maxgraph.sdkcommon.manager;

import com.alibaba.maxgraph.sdkcommon.MaxGraphClient;

public class ExecuteAlterEdgeRelationManager extends ExecuteSchemaManager {
    public ExecuteAlterEdgeRelationManager(String edgeLabel, MaxGraphClient client) {
        super(client);
        this.executeBuilder.append("graph.alterEdgeRelation(\"").append(edgeLabel).append("\")");
    }

    public ExecuteAlterEdgeRelationManager addRelation(String src, String dst) {
        this.executeBuilder.append(".addRelation(\"").append(src).append("\", \"").append(dst).append("\")");
        return this;
    }

    public ExecuteAlterEdgeRelationManager dropRelation(String src, String dst) {
        this.executeBuilder.append(".dropRelation(\"").append(src).append("\", \"").append(dst).append("\")");
        return this;
    }
}
