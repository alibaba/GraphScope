package com.alibaba.maxgraph.v2.frontend.compiler.optimizer;

import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.cost.CostModelManager;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.TreeNodeLabelManager;
import com.google.common.collect.Maps;
import org.apache.commons.configuration.Configuration;

import java.util.Map;

public class ContextManager {
    private Map<String, LogicalVertex> storeVertexList = Maps.newHashMap();
    private Configuration queryConfig;
    private VertexIdManager vertexIdManager;
    private TreeNodeLabelManager treeNodeLabelManager;
    private CostModelManager costModelManager;
    private GraphSchema schema;

    public ContextManager(CostModelManager costModelManager,
                          Configuration queryConfig,
                          VertexIdManager vertexIdManager,
                          TreeNodeLabelManager treeNodeLabelManager,
                          GraphSchema schema) {
        this.costModelManager = costModelManager;
        this.queryConfig = queryConfig;
        this.vertexIdManager = vertexIdManager;
        this.treeNodeLabelManager = treeNodeLabelManager;
        this.schema = schema;
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

    public GraphSchema getSchema() {
        return this.schema;
    }
}
