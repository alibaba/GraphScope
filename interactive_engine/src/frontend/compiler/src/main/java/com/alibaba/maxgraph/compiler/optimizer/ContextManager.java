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
package com.alibaba.maxgraph.compiler.optimizer;

import com.alibaba.maxgraph.compiler.cost.CostModelManager;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.compiler.tree.TreeNodeLabelManager;
import com.google.common.collect.Maps;
import org.apache.commons.configuration.Configuration;

import java.util.Map;

public class ContextManager {
    private Map<String, LogicalVertex> storeVertexList = Maps.newHashMap();
    private Configuration queryConfig;
    private VertexIdManager vertexIdManager;
    private TreeNodeLabelManager treeNodeLabelManager;
    private CostModelManager costModelManager;

    public ContextManager(CostModelManager costModelManager,
                          Configuration queryConfig,
                          VertexIdManager vertexIdManager,
                          TreeNodeLabelManager treeNodeLabelManager) {
        this.costModelManager = costModelManager;
        this.queryConfig = queryConfig;
        this.vertexIdManager = vertexIdManager;
        this.treeNodeLabelManager = treeNodeLabelManager;
    }

    public CostModelManager getCostModelManager() {
        return this.costModelManager;
    }

    public void addStoreVertex(String sideEffectKey, LogicalVertex vertex) {
        storeVertexList.put(sideEffectKey, vertex);
    }

    public LogicalVertex getStoreVertex(String sideEffectKey) {
        return storeVertexList.get(sideEffectKey);
    }

    public Configuration getQueryConfig() {
        return queryConfig;
    }

    public VertexIdManager getVertexIdManager() {
        return vertexIdManager;
    }

    public TreeNodeLabelManager getTreeNodeLabelManager() {
        return treeNodeLabelManager;
    }
}
