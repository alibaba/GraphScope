package com.alibaba.maxgraph.v2.frontend.compiler.tree;

import com.alibaba.maxgraph.proto.v2.LogicalCompare;
import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.SchemaElement;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.VertexType;
import com.alibaba.maxgraph.v2.common.util.PkHasher;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.edge.EdgeShuffleType;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorFilterFunction;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.addition.ExtendPropLocalNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.VertexValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.utils.CompilerUtils;
import com.alibaba.maxgraph.v2.frontend.compiler.utils.SchemaUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class HasTreeNode extends UnaryTreeNode implements ExtendPropLocalNode {
    private List<HasContainer> hasContainerList;

    public HasTreeNode(TreeNode input, List<HasContainer> hasContainerList, GraphSchema schema) {
        super(input, NodeType.FILTER, schema);

        checkNotNull(hasContainerList, "has container can't be null");
        optimizeContainerList(hasContainerList);
    }

    private void optimizeContainerList(List<HasContainer> hasContainerList) {
        this.hasContainerList = Lists.newArrayList();
        HasContainer filterLabel = null;
        for (HasContainer hasContainer : hasContainerList) {
            if (hasContainer.getPredicate() != null &&
                    hasContainer.getBiPredicate() == Compare.eq &&
                    StringUtils.equals(hasContainer.getKey(), T.label.getAccessor())) {
                try {
                    schema.getSchemaElement(hasContainer.getValue().toString());
                    filterLabel = hasContainer;
                } catch (Exception ignored) {
                }
            }
        }
        if (null != filterLabel) {
            String vertexLabel = filterLabel.getValue().toString();
            SchemaElement element = schema.getSchemaElement(vertexLabel);
            if (element instanceof VertexType) {
                VertexType vertexType = (VertexType) element;
                Map<Integer, HasContainer> pkContainerList = Maps.newHashMap();
                for (HasContainer hasContainer : hasContainerList) {
                    if (hasContainer.getPredicate() != null &&
                            hasContainer.getBiPredicate() == Compare.eq &&
                            SchemaUtils.checkPropExist(hasContainer.getKey(), schema)) {
                        int propId = SchemaUtils.getPropId(hasContainer.getKey(), schema);
                        if (SchemaUtils.isPropPrimaryKey(propId, schema, vertexType)) {
                            pkContainerList.put(propId, hasContainer);
                        }
                    }
                }
                List<Integer> primaryIdList = SchemaUtils.getVertexPrimaryKeyList(vertexType);
                if (ListUtils.isEqualList(primaryIdList, pkContainerList.keySet())) {
                    hasContainerList.remove(filterLabel);
                    hasContainerList.removeAll(pkContainerList.values());
                    List<String> pkValueList = Lists.newArrayList();
                    for (Integer pkPropId : primaryIdList) {
                        pkValueList.add(pkContainerList.get(pkPropId).getValue().toString());
                    }
                    PkHasher pkHasher = new PkHasher();
                    long vertexId = pkHasher.hash(vertexType.getLabelId(), pkValueList);
                    HasContainer vertexIdContainer = new HasContainer(T.id.getAccessor(), P.eq(vertexId));
                    this.hasContainerList.add(vertexIdContainer);
                }
            }
        }
        this.hasContainerList.addAll(hasContainerList);
    }

    public List<HasContainer> getHasContainerList() {
        return hasContainerList;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        List<LogicalCompare> logicalCompareList = Lists.newArrayList();
        hasContainerList.forEach(v -> logicalCompareList.add(CompilerUtils.parseLogicalCompare(v, schema, contextManager.getTreeNodeLabelManager().getLabelIndexList(), getInputNode().getOutputValueType() instanceof VertexValueType)));
        ProcessorFilterFunction filterFunction = new ProcessorFilterFunction(
                logicalCompareList.size() == 1 && logicalCompareList.get(0).getPropId() == 0 ? OperatorType.FILTER : OperatorType.HAS);
        filterFunction.getLogicalCompareList().addAll(logicalCompareList);

        boolean filterExchangeFlag = false;
        for (HasContainer hasContainer : this.hasContainerList) {
            if (SchemaUtils.checkPropExist(hasContainer.getKey(), schema)) {
                filterExchangeFlag = true;
                break;
            }
        }
        if (filterExchangeFlag) {
            return parseSingleUnaryVertex(contextManager.getVertexIdManager(), contextManager.getTreeNodeLabelManager(), filterFunction, contextManager);
        } else {
            return parseSingleUnaryVertex(contextManager.getVertexIdManager(), contextManager.getTreeNodeLabelManager(), filterFunction, contextManager, new LogicalEdge(EdgeShuffleType.FORWARD));
        }
    }

    @Override
    public boolean isPropLocalFlag() {
        return true;
    }

    @Override
    public ValueType getOutputValueType() {
        return getInputNode().getOutputValueType();
    }
}
