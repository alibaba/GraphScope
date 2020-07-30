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
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * client.createVertexType("person")
 * .addProperty("id", "long")
 * .addProperty("name", "string")
 * .addProperty("age", "int")
 * .storage("memory")
 * .comment("knows edge")
 * .primaryKey("id", "name")
 * .execute();
 */
public class ExecuteVertexSchemaManager extends ExecuteSchemaManager {

    public ExecuteVertexSchemaManager(String vertexType, MaxGraphClient client) {
        super(client);
        this.executeBuilder.append("graph.createVertexType(\"").append(vertexType).append("\")");
    }

    public ExecuteVertexSchemaManager addProperty(String name, String dataType) {
        this.executeBuilder.append(".addProperty(\"").append(name).append("\", ").append("\"").append(dataType).append("\")");
        return this;
    }

    public ExecuteVertexSchemaManager addProperty(String name, String dataType, String comment) {
        this.executeBuilder.append(".addProperty(\"").append(name).append("\", ").append("\"").append(dataType).append("\", \"").append(comment).append("\")");
        return this;
    }

    public ExecuteVertexSchemaManager addProperty(String name, String dataType, String comment, Object defaultValue) {
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

    public ExecuteVertexSchemaManager storage(String storage) {
        this.executeBuilder.append(".storage(\"").append(storage).append("\")");
        return this;
    }

    public ExecuteVertexSchemaManager comment(String comment) {
        this.executeBuilder.append(".comment(\"").append(comment).append("\")");
        return this;
    }

    public ExecuteVertexSchemaManager primaryKey(String... primaryKeyList) {
        List<String> primaryKeyValueList = Lists.newArrayList();
        for (String primary : primaryKeyList) {
            primaryKeyValueList.add("\"" + primary + "\"");
        }
        this.executeBuilder.append(".primaryKey(").append(StringUtils.join(primaryKeyValueList, ", ")).append(")");
        return this;
    }
}
