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

/**
 * client.createEdgeType("knows")
 * .addProperty("id", "long")
 * .addProperty("weight", "double")
 * .addRelation("person", "person")
 * .addRelation("person", "software")
 * .storage("memory")
 * .comment("knows edge")
 * .execute();
 */
public class ExecuteEdgeSchemaManager extends ExecuteSchemaManager {

    public ExecuteEdgeSchemaManager(String edgeType, MaxGraphClient client) {
        super(client);
        this.executeBuilder.append("graph.createEdgeType(\"").append(edgeType).append("\")");
    }

    public ExecuteEdgeSchemaManager addProperty(String name, String dataType) {
        this.executeBuilder.append(".addProperty(\"").append(name).append("\", ").append("\"").append(dataType).append("\")");
        return this;
    }

    public ExecuteEdgeSchemaManager addProperty(String name, String dataType, String comment) {
        this.executeBuilder.append(".addProperty(\"").append(name).append("\", ").append("\"").append(dataType).append("\", \"").append(comment).append("\")");
        return this;
    }

    public ExecuteEdgeSchemaManager addProperty(String name, String dataType, String comment, Object defaultValue) {
        this.executeBuilder.append(".addProperty(\"").append(name).append("\", ").append("\"").append(dataType).append("\", \"").append(comment).append("\"");
        if (defaultValue == null) {
            this.executeBuilder.append(")");
        } else if (defaultValue instanceof String) {
            this.executeBuilder.append(", \"").append(defaultValue.toString()).append("\")");
        } else {
            this.executeBuilder.append(", ").append(defaultValue.toString()).append(")");
        }
        return this;
    }

    public ExecuteEdgeSchemaManager storage(String storage) {
        this.executeBuilder.append(".storage(\"").append(storage).append("\")");
        return this;
    }

    public ExecuteEdgeSchemaManager comment(String comment) {
        this.executeBuilder.append(".comment(\"").append(comment).append("\")");
        return this;
    }

    public ExecuteEdgeSchemaManager addRelation(String srcLabel, String dstLabel) {
        this.executeBuilder.append(".addRelation(\"").append(srcLabel).append("\", \"").append(dstLabel).append("\")");
        return this;
    }
}
