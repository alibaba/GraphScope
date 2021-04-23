package com.alibaba.maxgraph.v2.frontend.compiler.tree.source;

import com.alibaba.maxgraph.proto.v2.Value;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.common.util.PkHasher;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSourceVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorSourceFunction;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.BaseTreeNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.NodeType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.TreeConstants;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.TreeNodeLabelManager;
import com.alibaba.maxgraph.v2.frontend.compiler.utils.CompilerUtils;
import com.alibaba.maxgraph.v2.frontend.compiler.utils.SchemaUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.T;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

public abstract class SourceTreeNode extends BaseTreeNode {
    private static final Logger logger = LoggerFactory.getLogger(SourceTreeNode.class);

    private Object initialSackValue;
    private Object[] ids;

    SourceTreeNode(Object[] ids, GraphSchema schema) {
        super(NodeType.SOURCE, schema);
        this.ids = ids;
    }

    public void setInitialSackValue(Object initialSackValue) {
        this.initialSackValue = initialSackValue;
    }

    @Override
    public void addHasContainer(HasContainer hasContainer) {
        if (null == ids
                && hasContainer.getKey() == T.id.getAccessor()
                && hasContainer.getPredicate() != null
                && (hasContainer.getBiPredicate() == Compare.eq || hasContainer.getBiPredicate() == Contains.within)) {
            if (hasContainer.getBiPredicate() == Compare.eq) {
                ids = new Object[]{hasContainer.getValue()};
            } else if (null != hasContainer.getValue() && hasContainer.getValue() instanceof Collection) {
                Collection valueList = (Collection) hasContainer.getValue();
                ids = valueList.toArray();
            }
        } else {
            this.hasContainerList.add(hasContainer);
        }
    }

    boolean processLabelArgument(Value.Builder argumentBuilder, boolean vertex) {
        HasContainer labelFilterContainer = null;
        for (HasContainer hasContainer : hasContainerList) {
            if (StringUtils.equals(hasContainer.getKey(), T.label.getAccessor())) {
                if (hasContainer.getBiPredicate() instanceof Compare
                        && hasContainer.getBiPredicate() == Compare.eq) {
                    argumentBuilder.addIntValueList(schema.getSchemaElement(hasContainer.getValue().toString()).getLabelId());
                } else if (hasContainer.getBiPredicate() instanceof Contains
                        && hasContainer.getBiPredicate() == Contains.within) {
                    for (String typeName : (List<String>) hasContainer.getValue()) {
                        try {
                            argumentBuilder.addIntValueList(schema.getSchemaElement(typeName).getLabelId());
                        } catch (Exception e) {
                            logger.error("There's no vertex with type => " + typeName);
                            argumentBuilder.addIntValueList(TreeConstants.MAGIC_PROP_ID);
                        }
                    }
                } else {
                    continue;
                }

                labelFilterContainer = hasContainer;
                break;
            }
        }
        if (null != labelFilterContainer) {
            hasContainerList.remove(labelFilterContainer);
            return true;
        }

        return false;
    }

    public Object[] getIds() {
        return ids;
    }

    boolean processIdArgument(Value.Builder argumentBuilder, boolean isVertex) {
        if (ids != null) {
            if (ids.length == 0) {
                argumentBuilder.clearIntValueList()
                        .addIntValueList(0);
            } else {
                for (Object id : ids) {

                    if (id instanceof Collection) {
                        Collection<Object> collection = (Collection) id;
                        if (collection.isEmpty()) {
                            argumentBuilder.clearIntValueList()
                                    .addIntValueList(0);
                        } else {
                            argumentBuilder.addAllLongValueList((collection)
                                    .stream()
                                    .map(v -> CompilerUtils.parseLongIdValue(id, isVertex))
                                    .collect(Collectors.toList()));
                        }
                    } else {
                        argumentBuilder.addLongValueList(CompilerUtils.parseLongIdValue(id, isVertex));
                    }
                }
            }
            return true;
        }
        return false;
    }

    void processPrimaryKey(Value.Builder argumentBuilder,
                           int vertexLabelId,
                           List<Integer> primaryKeyIdList,
                           Map<Integer, HasContainer> primaryContainerList) {
        Map<Integer, String> primaryKeyValueList = Maps.newHashMap();
        List<String> singlePrimaryKeyValueList = Lists.newArrayList();
        HasContainer withinContainer = null;
        for (HasContainer hasContainer : hasContainerList) {
            if (hasContainer.getPredicate() != null &&
                    (hasContainer.getBiPredicate() == Compare.eq ||
                            hasContainer.getBiPredicate() == Contains.within)) {
                int propId = SchemaUtils.getPropId(hasContainer.getKey(), schema);
                if (primaryKeyIdList.contains(propId)) {
                    if (hasContainer.getBiPredicate() == Compare.eq) {
                        primaryKeyValueList.put(propId, hasContainer.getValue().toString());
                        primaryContainerList.put(propId, hasContainer);
                    } else {
                        if (withinContainer == null && primaryKeyIdList.size() == 1) {
                            List<Object> valueList = (List<Object>) hasContainer.getValue();
                            valueList.forEach(v -> singlePrimaryKeyValueList.add(v.toString()));
                            withinContainer = hasContainer;
                        }
                    }
                }
            }
        }
        if (!singlePrimaryKeyValueList.isEmpty() || primaryKeyValueList.keySet().containsAll(primaryKeyIdList)) {
            PkHasher pkHasher = new PkHasher();
            if (!singlePrimaryKeyValueList.isEmpty()) {
                argumentBuilder.addAllLongValueList(singlePrimaryKeyValueList.stream()
                        .map(v -> pkHasher.hash(vertexLabelId, Lists.newArrayList(v)))
                        .collect(Collectors.toList()));
                hasContainerList.remove(withinContainer);
            } else {
                List<String> primaryValueList = Lists.newArrayList();
                for (Integer pkId : primaryKeyIdList) {
                    primaryValueList.add(primaryKeyValueList.get(pkId));
                }

                argumentBuilder.addLongValueList(pkHasher.hash(vertexLabelId, primaryValueList));
            }
        }
    }

    protected LogicalSubQueryPlan processSourceVertex(VertexIdManager vertexIdManager, TreeNodeLabelManager labelManager, LogicalSubQueryPlan logicalQueryPlan, ProcessorSourceFunction processorSourceFunction) {
        LogicalSourceVertex logicalSourceVertex = new LogicalSourceVertex(vertexIdManager.getId(), processorSourceFunction);

        logicalSourceVertex.getBeforeRequirementList().addAll(buildBeforeRequirementList(labelManager));
        logicalSourceVertex.getAfterRequirementList().addAll(buildAfterRequirementList(labelManager));
        getUsedLabelList().forEach(v -> processorSourceFunction.getUsedLabelList().add(labelManager.getLabelIndex(v)));

        logicalQueryPlan.addLogicalVertex(logicalSourceVertex);
        setFinishVertex(logicalQueryPlan.getOutputVertex(), labelManager);

        return logicalQueryPlan;
    }
}
