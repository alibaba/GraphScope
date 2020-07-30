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
package com.alibaba.maxgraph.sdkcommon.compiler.custom.graph;

import com.alibaba.maxgraph.Message;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class OdpsGraph implements MaxGraphSource {
    private String accessId;
    private String accessKey;
    private String endpoint;
    private List<Message.EdgeInput.Builder> edgeBuilderList = Lists.newArrayList();

    private OdpsGraph(String accessId, String accessKey, String endpoint) {
        this.accessId = accessId;
        this.accessKey = accessKey;
        this.endpoint = endpoint;
    }

    public static OdpsGraph access(String accessId, String accessKey, String endpoint) {
        return new OdpsGraph(accessId, accessKey, endpoint);
    }

    @Override
    public OdpsGraph addVertex(String name, String project, String table, String primaryKey) {
        throw new UnsupportedOperationException("addVertex");
    }

    private Message.EdgeInput.Builder getEdgeBuilder() {
        checkArgument(edgeBuilderList.size() > 0, "There's no edge yet.");
        return edgeBuilderList.get(edgeBuilderList.size() - 1);
    }

    @Override
    public OdpsGraph addEdge(String name, String project, String edgeTable) {
        edgeBuilderList.add(Message.EdgeInput.newBuilder().setTypeName(name)
                .setProject(project).setTable(edgeTable));
        return this;
    }

    @Override
    public OdpsGraph startType(String startVertex) {
        getEdgeBuilder().setSrcTypeName(startVertex);
        return this;
    }

    @Override
    public OdpsGraph startPrimaryKey(String primaryKey) {
        getEdgeBuilder().setSrcPrimaryKey(primaryKey);
        return this;
    }

    @Override
    public OdpsGraph endType(String endVertex) {
        getEdgeBuilder().setDstTypeName(endVertex);
        return this;
    }

    @Override
    public OdpsGraph endPrimaryKey(String primaryKey) {
        getEdgeBuilder().setDstPrimaryKey(primaryKey);
        return this;
    }

    @Override
    public OdpsGraph direction(String direction) {
        getEdgeBuilder().setDirection(Message.EdgeDirection.valueOf("DIR_" + StringUtils.upperCase(direction)));
        return this;
    }

    public String getAccessId() {
        return accessId;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public List<Message.EdgeInput.Builder> getEdgeBuilderList() {
        return edgeBuilderList;
    }
}
