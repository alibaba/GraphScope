/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.compiler.tree.source;

import com.alibaba.maxgraph.Message;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.tree.TreeNodeLabelManager;
import com.alibaba.maxgraph.compiler.logical.LogicalSourceVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorSourceFunction;
import com.alibaba.maxgraph.compiler.optimizer.OperatorListManager;
import com.alibaba.maxgraph.compiler.tree.BaseTreeNode;
import com.alibaba.maxgraph.compiler.tree.NodeType;
import com.alibaba.maxgraph.compiler.tree.TreeConstants;
import com.alibaba.maxgraph.compiler.utils.CompilerUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.T;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
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
                && (hasContainer.getBiPredicate() == Compare.eq
                        || hasContainer.getBiPredicate() == Contains.within)) {
            if (hasContainer.getBiPredicate() == Compare.eq) {
                ids = new Object[] {hasContainer.getValue()};
            } else {
                List valueList = (List) hasContainer.getValue();
                if (valueList.isEmpty()) {
                    this.hasContainerList.add(hasContainer);
                } else {
                    ids = valueList.toArray();
                }
            }
        } else {
            this.hasContainerList.add(hasContainer);
        }
    }

    boolean processLabelArgument(Message.Value.Builder argumentBuilder, boolean vertex) {
        HasContainer labelFilterContainer = null;
        for (HasContainer hasContainer : hasContainerList) {
            if (StringUtils.equals(hasContainer.getKey(), T.label.getAccessor())) {
                if (hasContainer.getBiPredicate() instanceof Compare
                        && hasContainer.getBiPredicate() == Compare.eq) {
                    argumentBuilder.addIntValueList(
                            schema.getElement(hasContainer.getValue().toString()).getLabelId());
                } else if (hasContainer.getBiPredicate() instanceof Contains
                        && hasContainer.getBiPredicate() == Contains.within) {
                    for (String typeName : (List<String>) hasContainer.getValue()) {
                        try {
                            argumentBuilder.addIntValueList(
                                    schema.getElement(typeName).getLabelId());
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

    boolean processIdArgument(Message.Value.Builder argumentBuilder, boolean isVertex) {
        if (ids != null) {
            if (ids.length == 0) {
                argumentBuilder.clearLongValueList().addLongValueList(Long.MAX_VALUE);
            } else {
                for (Object id : ids) {
                    if (OperatorListManager.isPrepareValue(id.toString())) {
                        throw new IllegalArgumentException();
                    } else {
                        if (id instanceof Collection) {
                            Collection<Object> collection = (Collection) id;
                            if (collection.isEmpty()) {
                                argumentBuilder
                                        .clearLongValueList()
                                        .addLongValueList(Long.MAX_VALUE);
                            } else {
                                argumentBuilder.addAllLongValueList(
                                        (collection)
                                                .stream()
                                                        .map(
                                                                v ->
                                                                        CompilerUtils
                                                                                .parseLongIdValue(
                                                                                        id,
                                                                                        isVertex))
                                                        .collect(Collectors.toList()));
                            }
                        } else {
                            argumentBuilder.addLongValueList(
                                    CompilerUtils.parseLongIdValue(id, isVertex));
                        }
                    }
                }
            }
            return true;
        }
        return false;
    }

    protected LogicalSubQueryPlan processSourceVertex(
            VertexIdManager vertexIdManager,
            TreeNodeLabelManager labelManager,
            LogicalSubQueryPlan logicalQueryPlan,
            ProcessorSourceFunction processorSourceFunction) {
        LogicalSourceVertex logicalSourceVertex =
                new LogicalSourceVertex(vertexIdManager.getId(), processorSourceFunction);

        logicalSourceVertex
                .getBeforeRequirementList()
                .addAll(buildBeforeRequirementList(labelManager));
        logicalSourceVertex
                .getAfterRequirementList()
                .addAll(buildAfterRequirementList(labelManager));
        getUsedLabelList()
                .forEach(
                        v ->
                                processorSourceFunction
                                        .getUsedLabelList()
                                        .add(labelManager.getLabelIndex(v)));

        logicalQueryPlan.addLogicalVertex(logicalSourceVertex);
        setFinishVertex(logicalQueryPlan.getOutputVertex(), labelManager);

        return logicalQueryPlan;
    }
}
