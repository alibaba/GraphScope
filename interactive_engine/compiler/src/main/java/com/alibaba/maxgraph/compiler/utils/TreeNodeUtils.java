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
package com.alibaba.maxgraph.compiler.utils;

import com.alibaba.maxgraph.Message;
import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.common.util.SchemaUtils;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.logical.LogicalBinaryVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.compiler.logical.LogicalQueryPlan;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.LogicalUnaryVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFilterFunction;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.CountGlobalTreeNode;
import com.alibaba.maxgraph.compiler.tree.FoldTreeNode;
import com.alibaba.maxgraph.compiler.tree.HasTreeNode;
import com.alibaba.maxgraph.compiler.tree.MaxTreeNode;
import com.alibaba.maxgraph.compiler.tree.MinTreeNode;
import com.alibaba.maxgraph.compiler.tree.NodeType;
import com.alibaba.maxgraph.compiler.tree.PropertyMapTreeNode;
import com.alibaba.maxgraph.compiler.tree.RangeGlobalTreeNode;
import com.alibaba.maxgraph.compiler.tree.SelectOneTreeNode;
import com.alibaba.maxgraph.compiler.tree.SumTreeNode;
import com.alibaba.maxgraph.compiler.tree.TokenTreeNode;
import com.alibaba.maxgraph.compiler.tree.TreeConstants;
import com.alibaba.maxgraph.compiler.tree.TreeNode;
import com.alibaba.maxgraph.compiler.tree.TreeNodeLabelManager;
import com.alibaba.maxgraph.compiler.tree.UnaryTreeNode;
import com.alibaba.maxgraph.compiler.tree.VertexTreeNode;
import com.alibaba.maxgraph.compiler.tree.addition.AbstractUseKeyNode;
import com.alibaba.maxgraph.compiler.tree.addition.PropertyNode;
import com.alibaba.maxgraph.compiler.tree.source.SourceDelegateNode;
import com.alibaba.maxgraph.compiler.tree.source.SourceTreeNode;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class TreeNodeUtils {
    /**
     * Parse and get property/label id from tree node
     *
     * @param treeNode     The given tree node
     * @param schema       The given schema
     * @param labelManager The given label manager
     * @return The optional property/label id
     */
    public static Optional<Integer> parseValueDirectlyNode(TreeNode treeNode, GraphSchema schema, TreeNodeLabelManager labelManager) {
        if (null == treeNode || treeNode instanceof SourceDelegateNode) {
            return Optional.of(0);
        } else if (treeNode instanceof PropertyMapTreeNode) {
            return Optional.empty();
        } else if (((UnaryTreeNode) treeNode).getInputNode() instanceof SourceDelegateNode) {
            if (treeNode instanceof PropertyNode) {
                return Optional.of(SchemaUtils.getPropId(PropertyNode.class.cast(treeNode).getPropKeyList().iterator().next(), schema));
            } else if (treeNode instanceof SelectOneTreeNode) {
                return Optional.of(labelManager.getLabelIndex(((SelectOneTreeNode) treeNode).getSelectLabel()));
            } else if (treeNode instanceof TokenTreeNode) {
                return Optional.of(labelManager.getLabelIndex(((TokenTreeNode) treeNode).getToken().getAccessor()));
            } else {
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }
    }

    /**
     * Parse aggregate type from given tree node
     *
     * @param treeNode The given tree node
     * @return The operator type
     */
    public static QueryFlowOuterClass.OperatorType parseAggregateOperatorType(TreeNode treeNode) {
        if (treeNode instanceof CountGlobalTreeNode) {
            return QueryFlowOuterClass.OperatorType.COUNT;
        } else if (treeNode instanceof MaxTreeNode) {
            return QueryFlowOuterClass.OperatorType.MAX;
        } else if (treeNode instanceof MinTreeNode) {
            return QueryFlowOuterClass.OperatorType.MIN;
        } else if (treeNode instanceof SumTreeNode) {
            return QueryFlowOuterClass.OperatorType.SUM;
        } else if (treeNode instanceof FoldTreeNode) {
            return QueryFlowOuterClass.OperatorType.FOLD;
        } else {
            throw new IllegalArgumentException("Only support aggregate here for input " + treeNode.toString());
        }
    }

    /**
     * Add limit(1) to multiple output tree node
     *
     * @param treeNode The given tree node
     * @param schema   The schema
     * @return The result tree node
     */
    public static TreeNode buildSingleOutputNode(TreeNode treeNode, GraphSchema schema) {
        if (null == treeNode) {
            return null;
        }
        boolean hasMultipleEachInputFlag = TreeNodeUtils.checkMultipleOutput(treeNode);
        TreeNode currentFilterTreeNode = treeNode;
        if (hasMultipleEachInputFlag) {
            currentFilterTreeNode = new RangeGlobalTreeNode(treeNode, schema, 0, 1);
        }

        return currentFilterTreeNode;
    }

    /**
     * Parse enter key argument from given tree node
     *
     * @param treeNode             The given tree node
     * @param schema               The given schema
     * @param treeNodeLabelManager The tree node label manager
     * @return The enter key argument
     */
    public static Optional<QueryFlowOuterClass.EnterKeyArgumentProto> parseEnterKeyArgument(
            TreeNode treeNode,
            GraphSchema schema,
            TreeNodeLabelManager treeNodeLabelManager) {
        if (treeNode instanceof PropertyMapTreeNode) {
            QueryFlowOuterClass.EnterKeyArgumentProto.Builder builder = QueryFlowOuterClass.EnterKeyArgumentProto.newBuilder()
                    .setEnterKeyType(QueryFlowOuterClass.EnterKeyTypeProto.KEY_PROP_VAL_MAP)
                    .setUniqFlag(false)
                    .addAllPropIdList(((PropertyMapTreeNode) treeNode).getPropKeyList()
                            .stream()
                            .map(v -> SchemaUtils.getPropId(v, schema))
                            .collect(Collectors.toList()));
            return Optional.of(builder.build());
        } else if (treeNode instanceof PropertyNode) {
            String propName = PropertyNode.class.cast(treeNode).getPropKeyList().iterator().next();
            QueryFlowOuterClass.EnterKeyArgumentProto.Builder builder = QueryFlowOuterClass.EnterKeyArgumentProto.newBuilder()
                    .setEnterKeyType(QueryFlowOuterClass.EnterKeyTypeProto.KEY_PROP_LABEL)
                    .setUniqFlag(false)
                    .setPropLabelId(SchemaUtils.getPropId(propName, schema));
            return Optional.of(builder.build());
        } else if (treeNode instanceof SelectOneTreeNode) {
            QueryFlowOuterClass.EnterKeyArgumentProto.Builder builder = QueryFlowOuterClass.EnterKeyArgumentProto.newBuilder()
                    .setEnterKeyType(QueryFlowOuterClass.EnterKeyTypeProto.KEY_PROP_LABEL)
                    .setUniqFlag(false)
                    .setPropLabelId(treeNodeLabelManager.getLabelIndex(SelectOneTreeNode.class.cast(treeNode).getSelectLabel()));
            return Optional.of(builder.build());
        } else if (treeNode instanceof TokenTreeNode) {
            QueryFlowOuterClass.EnterKeyArgumentProto.Builder builder = QueryFlowOuterClass.EnterKeyArgumentProto.newBuilder()
                    .setEnterKeyType(QueryFlowOuterClass.EnterKeyTypeProto.KEY_PROP_LABEL)
                    .setUniqFlag(false)
                    .setPropLabelId(treeNodeLabelManager.getLabelIndex(TokenTreeNode.class.cast(treeNode).getToken().getAccessor()));
            return Optional.of(builder.build());
        } else {
            return Optional.empty();
        }
    }

    /**
     * Build query plan from given leaf tree node
     *
     * @param treeNode             The given leaf tree node
     * @param treeNodeLabelManager The tree node label manager
     * @param contextManager       The context manager
     * @param vertexIdManager      The vertex id manager
     * @return The query plan of sub query
     */
    public static LogicalSubQueryPlan buildQueryPlanWithSource(TreeNode treeNode,
                                                               TreeNodeLabelManager treeNodeLabelManager,
                                                               ContextManager contextManager,
                                                               VertexIdManager vertexIdManager,
                                                               LogicalVertex sourceVertex) {
        List<TreeNode> treeNodeList = buildTreeNodeListFromLeaf(treeNode);
        LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
        for (TreeNode currentNode : treeNodeList) {
            if (currentNode instanceof SourceDelegateNode) {
                currentNode.setFinishVertex(sourceVertex, treeNodeLabelManager);
            }
            logicalSubQueryPlan.mergeLogicalQueryPlan(currentNode.buildLogicalQueryPlan(contextManager));
        }
        return logicalSubQueryPlan;
    }

    /**
     * Build query plan from given leaf tree node
     *
     * @param treeNode             The given leaf tree node
     * @param treeNodeLabelManager The tree node label manager
     * @param contextManager       The context manager
     * @param vertexIdManager      The vertex id manager
     * @return The query plan of sub query
     */
    public static LogicalSubQueryPlan buildQueryPlan(TreeNode treeNode,
                                                     TreeNodeLabelManager treeNodeLabelManager,
                                                     ContextManager contextManager,
                                                     VertexIdManager vertexIdManager) {
        List<TreeNode> treeNodeList = buildTreeNodeListFromLeaf(treeNode);
        LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
        for (TreeNode currentNode : treeNodeList) {
            logicalSubQueryPlan.mergeLogicalQueryPlan(currentNode.buildLogicalQueryPlan(contextManager));
        }
        return logicalSubQueryPlan;
    }

    /**
     * Check if tree node list has use key, it will decide join or not later
     *
     * @param treeNodeList The given tree node list
     * @return The use key flag
     */
    public static boolean checkNodeListUseKey(List<TreeNode> treeNodeList) {
        for (TreeNode treeNode : treeNodeList) {
            if (treeNode instanceof AbstractUseKeyNode) {
                return true;
            }
        }

        return false;
    }

    /**
     * Build sub query plan from given leaf tree node
     *
     * @param treeNode       The given leaf tree node
     * @param sourceVertex   The source vertex
     * @param contextManager The context manager
     * @return The query plan of sub query
     */
    public static LogicalSubQueryPlan buildSubQueryPlan(TreeNode treeNode,
                                                        LogicalVertex sourceVertex,
                                                        ContextManager contextManager) {
        return buildSubQueryPlan(treeNode, sourceVertex, contextManager, true);
    }

    /**
     * Build sub query plan from given leaf tree node
     *
     * @param treeNode       The given leaf tree node
     * @param sourceVertex   The source vertex
     * @param contextManager The context manager
     * @param generateUseKey The flag of generated use key, it may be generated outside such as group
     * @return The query plan of sub query
     */
    public static LogicalSubQueryPlan buildSubQueryPlan(TreeNode treeNode,
                                                        LogicalVertex sourceVertex,
                                                        ContextManager contextManager,
                                                        boolean generateUseKey) {
        TreeNodeLabelManager treeNodeLabelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();

        List<TreeNode> treeNodeList = buildTreeNodeListFromLeaf(treeNode);
        boolean useKeyFlag = checkNodeListUseKey(treeNodeList);
        LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
        LogicalVertex currentSourceVertex = sourceVertex;
        for (TreeNode currentNode : treeNodeList) {
            if (currentNode instanceof AbstractUseKeyNode) {
                ((AbstractUseKeyNode) currentNode).enableUseKeyFlag(currentSourceVertex);
            }
            if (currentNode instanceof SourceDelegateNode) {
                if (useKeyFlag && generateUseKey) {
                    LogicalVertex enterKeyVertex = new LogicalUnaryVertex(
                            vertexIdManager.getId(),
                            new ProcessorFunction(
                                    QueryFlowOuterClass.OperatorType.ENTER_KEY,
                                    Message.Value.newBuilder().setPayload(
                                            QueryFlowOuterClass.EnterKeyArgumentProto.newBuilder()
                                                    .setEnterKeyType(QueryFlowOuterClass.EnterKeyTypeProto.KEY_SELF)
                                                    .setUniqFlag(true)
                                                    .build().toByteString())),
                            false,
                            currentSourceVertex);
                    currentNode.setFinishVertex(enterKeyVertex, treeNodeLabelManager);
                    logicalSubQueryPlan.addLogicalVertex(currentSourceVertex);
                    logicalSubQueryPlan.addLogicalVertex(enterKeyVertex);
                    logicalSubQueryPlan.addLogicalEdge(currentSourceVertex, enterKeyVertex, new LogicalEdge());
                    currentSourceVertex = enterKeyVertex;
                } else {
                    currentNode.setFinishVertex(currentSourceVertex, treeNodeLabelManager);
                    logicalSubQueryPlan.addLogicalVertex(currentSourceVertex);
                }
            }
            logicalSubQueryPlan.mergeLogicalQueryPlan(currentNode.buildLogicalQueryPlan(contextManager));
        }

        return logicalSubQueryPlan;
    }

    /**
     * Build sub query plan, and save the result of sub plan to given label id
     *
     * @param treeNode       The given tree node
     * @param sourceVertex   The given source vertex
     * @param contextManager The given context manager
     * @return The result query plan and label id
     */
    public static Pair<LogicalQueryPlan, Integer> buildSubQueryWithLabel(TreeNode treeNode,
                                                                         LogicalVertex sourceVertex,
                                                                         ContextManager contextManager) {
        boolean joinFlag = checkJoinSourceFlag(treeNode);
        List<TreeNode> treeNodeList = buildTreeNodeListFromLeaf(treeNode);
        LogicalQueryPlan queryPlan = new LogicalQueryPlan(contextManager);
        LogicalVertex originalVertex = null;
        LogicalVertex currentSourceVertex = sourceVertex;
        for (TreeNode currentNode : treeNodeList) {
            if (currentNode instanceof SourceDelegateNode) {
                queryPlan.addLogicalVertex(sourceVertex);
                if (joinFlag) {
                    LogicalVertex enterKeyVertex = new LogicalUnaryVertex(
                            contextManager.getVertexIdManager().getId(),
                            new ProcessorFunction(
                                    QueryFlowOuterClass.OperatorType.ENTER_KEY,
                                    Message.Value.newBuilder().setPayload(
                                            QueryFlowOuterClass.EnterKeyArgumentProto.newBuilder()
                                                    .setEnterKeyType(QueryFlowOuterClass.EnterKeyTypeProto.KEY_SELF)
                                                    .setUniqFlag(true)
                                                    .build().toByteString())),
                            false,
                            currentSourceVertex);
                    currentNode.setFinishVertex(enterKeyVertex, contextManager.getTreeNodeLabelManager());
                    queryPlan.addLogicalVertex(enterKeyVertex);
                    queryPlan.addLogicalEdge(sourceVertex, enterKeyVertex, new LogicalEdge());
                    currentSourceVertex = enterKeyVertex;
                    originalVertex = enterKeyVertex;
                } else {
                    currentNode.setFinishVertex(sourceVertex, contextManager.getTreeNodeLabelManager());
                    originalVertex = sourceVertex;
                }
            } else {
                queryPlan.mergeLogicalQueryPlan(currentNode.buildLogicalQueryPlan(
                        contextManager));
            }
        }
        checkNotNull(originalVertex, "original vertex can't be null");

        LogicalVertex valueVertex = queryPlan.getOutputVertex();
        String valueLabel = contextManager.getTreeNodeLabelManager().createSysLabelStart("val");
        int valueLabelId = contextManager.getTreeNodeLabelManager().getLabelIndex(valueLabel);
        if (joinFlag) {
            LogicalVertex joinVertex;
            if (treeNode instanceof CountGlobalTreeNode || treeNode instanceof SumTreeNode) {
                joinVertex = new LogicalBinaryVertex(
                        contextManager.getVertexIdManager().getId(),
                        new ProcessorFunction(QueryFlowOuterClass.OperatorType.JOIN_COUNT_LABEL,
                                Message.Value.newBuilder().setIntValue(valueLabelId)),
                        false,
                        originalVertex,
                        valueVertex);
            } else {
                joinVertex = new LogicalBinaryVertex(
                        contextManager.getVertexIdManager().getId(),
                        new ProcessorFunction(QueryFlowOuterClass.OperatorType.JOIN_LABEL,
                                Message.Value.newBuilder().setIntValue(valueLabelId)),
                        false,
                        originalVertex,
                        valueVertex);
            }
            queryPlan.addLogicalVertex(joinVertex);
            queryPlan.addLogicalEdge(originalVertex, joinVertex, LogicalEdge.shuffleByKey(0));
            queryPlan.addLogicalEdge(valueVertex, joinVertex, LogicalEdge.forwardEdge());
        } else {
            LogicalVertex originalOutputVertex = queryPlan.getTargetVertex(originalVertex);
            String originalLabel = contextManager.getTreeNodeLabelManager().createBeforeSysLabelStart(originalOutputVertex, "original");
            ProcessorFunction selectFunction = createSelectOneFunction(originalLabel, Pop.first, contextManager.getTreeNodeLabelManager().getLabelIndexList());
            LogicalVertex selectVertex = new LogicalUnaryVertex(contextManager.getVertexIdManager().getId(),
                    selectFunction,
                    valueVertex);
            selectVertex.getBeforeRequirementList().add(QueryFlowOuterClass.RequirementValue.newBuilder()
                    .setReqType(QueryFlowOuterClass.RequirementType.LABEL_START)
                    .setReqArgument(Message.Value.newBuilder()
                            .addIntValueList(valueLabelId)));
            queryPlan.addLogicalVertex(selectVertex);
            queryPlan.addLogicalEdge(valueVertex, selectVertex, LogicalEdge.forwardEdge());
        }

        return Pair.of(queryPlan, valueLabelId);
    }

    /**
     * @param treeNode             The given tree node
     * @param sourceVertex         The source vertex
     * @param treeNodeLabelManager The given label manager
     * @param contextManager       The context manager
     * @param vertexIdManager      The vertex id manager
     * @return The sub query with enter key vertex
     */
    public static LogicalSubQueryPlan buildSubQueryPlanWithKey(TreeNode treeNode,
                                                               LogicalVertex sourceVertex,
                                                               TreeNodeLabelManager treeNodeLabelManager,
                                                               ContextManager contextManager,
                                                               VertexIdManager vertexIdManager) {
        List<TreeNode> treeNodeList = buildTreeNodeListFromLeaf(treeNode);
        LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
        LogicalVertex currentSourceVertex = sourceVertex;
        for (TreeNode currentNode : treeNodeList) {
            if (currentNode instanceof AbstractUseKeyNode) {
                ((AbstractUseKeyNode) currentNode).enableUseKeyFlag(currentSourceVertex);
            }
            if (currentNode instanceof SourceDelegateNode) {
                LogicalVertex enterKeyVertex = new LogicalUnaryVertex(
                        vertexIdManager.getId(),
                        new ProcessorFunction(
                                QueryFlowOuterClass.OperatorType.ENTER_KEY,
                                Message.Value.newBuilder().setPayload(
                                        QueryFlowOuterClass.EnterKeyArgumentProto.newBuilder()
                                                .setEnterKeyType(QueryFlowOuterClass.EnterKeyTypeProto.KEY_SELF)
                                                .setUniqFlag(true)
                                                .build().toByteString())),
                        false,
                        currentSourceVertex);
                currentNode.setFinishVertex(enterKeyVertex, treeNodeLabelManager);
                logicalSubQueryPlan.addLogicalVertex(currentSourceVertex);
                logicalSubQueryPlan.addLogicalVertex(enterKeyVertex);
                logicalSubQueryPlan.addLogicalEdge(currentSourceVertex, enterKeyVertex, new LogicalEdge());
                currentSourceVertex = enterKeyVertex;

            }
            logicalSubQueryPlan.mergeLogicalQueryPlan(currentNode.buildLogicalQueryPlan(contextManager));
        }

        return logicalSubQueryPlan;
    }

    /**
     * Parse tree node list from leaf node
     *
     * @param leafNode The given leaf node
     * @return The tree node list
     */
    public static List<TreeNode> buildTreeNodeListFromLeaf(TreeNode leafNode) {
        TreeNode currentNode = leafNode;
        LinkedList<TreeNode> treeNodeList = Lists.newLinkedList();
        while (!(currentNode instanceof SourceTreeNode)) {
            treeNodeList.addFirst(currentNode);
            currentNode = ((UnaryTreeNode) currentNode).getInputNode();
        }
        treeNodeList.addFirst(currentNode);

        return treeNodeList;
    }

    /**
     * Get the source tree node from leaf node
     *
     * @param treeNode The given leaf node
     * @return The source tree node
     */
    public static TreeNode getSourceTreeNode(TreeNode treeNode) {
        List<TreeNode> treeNodeList = buildTreeNodeListFromLeaf(treeNode);
        return treeNodeList.get(0);
    }

    /**
     * Check if the tree node only have multiple output records for every input
     *
     * @param treeNode The given tree node
     * @return true if there're multiple output records
     */
    public static boolean checkMultipleOutput(TreeNode treeNode) {
        TreeNode currTreeNode = treeNode;
        while (!(currTreeNode instanceof SourceTreeNode)) {
            if (currTreeNode.getNodeType() == NodeType.FLATMAP) {
                return true;
            }
            if (currTreeNode instanceof RangeGlobalTreeNode) {
                RangeGlobalTreeNode rangeGlobalTreeNode = RangeGlobalTreeNode.class.cast(currTreeNode);
                if (rangeGlobalTreeNode.getHigh() - rangeGlobalTreeNode.getLow() > 1) {
                    return true;
                } else {
                    return false;
                }
            }
            if (currTreeNode.getNodeType() == NodeType.AGGREGATE) {
                return false;
            }
            currTreeNode = UnaryTreeNode.class.cast(currTreeNode).getInputNode();
        }

        return false;
    }

    /**
     * If the result should use join to get the value
     *
     * @param treeNode The given tree node
     * @return The flag
     */
    public static boolean checkJoinSourceFlag(TreeNode treeNode) {
        return checkMultipleOutput(treeNode) || checkNodeListUseKey(buildTreeNodeListFromLeaf(treeNode));
    }

    /**
     * If the result should select input value
     */
    public static boolean checkSelectFlag(TreeNode treeNode) {
        List<TreeNode> treeNodeList = buildTreeNodeListFromLeaf(treeNode);
        for (TreeNode currNode : treeNodeList) {
            if (currNode instanceof SourceDelegateNode) {
                continue;
            }
            if (currNode.getNodeType() != NodeType.FILTER) {
                return true;
            }
        }

        return false;
    }

    /**
     * Optimize filter node in TraversalFilterNode
     *
     * @param filterTreeNode The given filter node
     * @return The optimized filter node
     */
    public static TreeNode optimizeSubFilterNode(TreeNode filterTreeNode) {
        TreeNode sourceNode = getSourceTreeNode(filterTreeNode);
        UnaryTreeNode firstNode = (UnaryTreeNode) sourceNode.getOutputNode();
        if (firstNode instanceof VertexTreeNode) {
            VertexTreeNode vertexTreeNode = (VertexTreeNode) firstNode;
            Direction direction = vertexTreeNode.getDirection();
            if (direction == Direction.OUT) {
                while (true) {
                    boolean optimizeFinish = true;
                    TreeNode outputNode = vertexTreeNode.getOutputNode();
                    if (null == outputNode) {
                        vertexTreeNode.enableCountFlag();
                        TreeNode hasTreeNode = new HasTreeNode(vertexTreeNode,
                                Lists.newArrayList(new HasContainer("", P.gt(0L))),
                                vertexTreeNode.getSchema());
                        hasTreeNode.setOutputNode(null);
                        break;
                    }
                    if (outputNode instanceof RangeGlobalTreeNode) {
                        TreeNode rangeOutputNode = outputNode.getOutputNode();
                        vertexTreeNode.setOutputNode(rangeOutputNode);
                        if (null != rangeOutputNode) {
                            ((UnaryTreeNode) rangeOutputNode).setInputNode(vertexTreeNode);
                        }
                        optimizeFinish = false;
                    } else if (outputNode instanceof CountGlobalTreeNode) {
                        vertexTreeNode.enableCountFlag();
                        TreeNode rangeOutputNode = outputNode.getOutputNode();
                        vertexTreeNode.setOutputNode(rangeOutputNode);
                        if (null != rangeOutputNode) {
                            ((UnaryTreeNode) rangeOutputNode).setInputNode(vertexTreeNode);
                        }
                        optimizeFinish = false;
                    }
                    if (optimizeFinish) {
                        break;
                    }
                }
            }
        }
        TreeNode currentFilterNode = sourceNode;
        while (currentFilterNode.getOutputNode() != null) {
            currentFilterNode = currentFilterNode.getOutputNode();
        }
        return currentFilterNode;
    }

    /**
     * Build logical plan with filter tree node and output the input value
     */
    public static LogicalVertex buildFilterTreeNode(TreeNode treeNode,
                                                    ContextManager contextManager,
                                                    LogicalQueryPlan logicalQueryPlan,
                                                    LogicalVertex sourceVertex,
                                                    GraphSchema schema) {
        TreeNodeLabelManager labelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        TreeNode filterTreeNode = TreeNodeUtils.optimizeSubFilterNode(treeNode);
        UnaryTreeNode unaryTreeNode = UnaryTreeNode.class.cast(filterTreeNode);
        LogicalVertex outputVertex;
        if (unaryTreeNode.getInputNode() instanceof SourceTreeNode &&
                (unaryTreeNode instanceof SelectOneTreeNode ||
                        unaryTreeNode instanceof PropertyNode)) {
            // optimize traversal filter to filter operator
            int propId;
            if (unaryTreeNode instanceof SelectOneTreeNode) {
                propId = labelManager.getLabelIndex(SelectOneTreeNode.class.cast(unaryTreeNode).getSelectLabel());
            } else {
                propId = SchemaUtils.getPropId(PropertyNode.class.cast(unaryTreeNode).getPropKeyList().iterator().next(), schema);
            }
            Message.LogicalCompare logicalCompare = Message.LogicalCompare.newBuilder()
                    .setCompare(Message.CompareType.EXIST)
                    .setPropId(propId).build();
            ProcessorFilterFunction processorFunction = new ProcessorFilterFunction(QueryFlowOuterClass.OperatorType.HAS);
            if (propId < 0) {
                processorFunction.getUsedLabelList().add(propId);
            }
            processorFunction.getLogicalCompareList().add(logicalCompare);
            outputVertex = new LogicalUnaryVertex(vertexIdManager.getId(), processorFunction, false, sourceVertex);
            logicalQueryPlan.addLogicalVertex(outputVertex);
            logicalQueryPlan.addLogicalEdge(sourceVertex, outputVertex, new LogicalEdge());
        } else {
            TreeNode currentFilterTreeNode = TreeNodeUtils.buildSingleOutputNode(filterTreeNode, schema);
            // build filter plan, and use join direct filter vertex to filter left stream
            LogicalSubQueryPlan filterPlan = TreeNodeUtils.buildSubQueryPlan(
                    currentFilterTreeNode,
                    sourceVertex,
                    contextManager);
            TreeNode filterSourceNode = TreeNodeUtils.getSourceTreeNode(currentFilterTreeNode);
            sourceVertex = filterSourceNode.getOutputVertex();

            LogicalVertex rightVertex = filterPlan.getOutputVertex();
            logicalQueryPlan.mergeLogicalQueryPlan(filterPlan);
            if (TreeNodeUtils.checkJoinSourceFlag(currentFilterTreeNode)) {
                LogicalBinaryVertex filterJoinVertex = new LogicalBinaryVertex(
                        vertexIdManager.getId(),
                        new ProcessorFunction(QueryFlowOuterClass.OperatorType.JOIN_DIRECT_FILTER),
                        false,
                        sourceVertex,
                        rightVertex);
                logicalQueryPlan.addLogicalVertex(filterJoinVertex);
                logicalQueryPlan.addLogicalEdge(sourceVertex, filterJoinVertex, new LogicalEdge());
                logicalQueryPlan.addLogicalEdge(rightVertex, filterJoinVertex, new LogicalEdge());
                outputVertex = filterJoinVertex;
            } else if (TreeNodeUtils.checkSelectFlag(currentFilterTreeNode)) {
                String inputLabel = labelManager.createSysLabelStart(sourceVertex, "input");
                ProcessorFunction selectFunction = createSelectOneFunction(inputLabel, Pop.last, labelManager.getLabelIndexList());
                LogicalUnaryVertex selectVertex = new LogicalUnaryVertex(
                        vertexIdManager.getId(),
                        selectFunction,
                        false,
                        rightVertex);
                logicalQueryPlan.addLogicalVertex(selectVertex);
                logicalQueryPlan.addLogicalEdge(rightVertex, selectVertex, new LogicalEdge());
                outputVertex = logicalQueryPlan.getOutputVertex();
            } else {
                outputVertex = logicalQueryPlan.getOutputVertex();
            }
        }

        return outputVertex;
    }

    /**
     * Build select function
     *
     * @param selectLabel    The given label
     * @param pop            The given pop
     * @param labelIndexList The given label index list
     * @return The select function
     */
    public static ProcessorFunction createSelectOneFunction(String selectLabel, Pop pop, Map<String, Integer> labelIndexList) {
        boolean labelExist = labelIndexList.containsKey(selectLabel);
        Message.Value.Builder argumentBuilder = Message.Value.newBuilder()
                .setBoolValue(labelExist)
                .setIntValue(null == pop ? Message.PopType.POP_EMPTY.getNumber() : Message.PopType.valueOf(StringUtils.upperCase(pop.name())).getNumber())
                .addIntValueList(labelExist ? labelIndexList.get(selectLabel) : TreeConstants.MAGIC_LABEL_ID)
                .setStrValue(selectLabel);
        ProcessorFunction processorFunction = new ProcessorFunction(QueryFlowOuterClass.OperatorType.SELECT_ONE, argumentBuilder);
        processorFunction.getUsedLabelList().add(labelExist ? labelIndexList.get(selectLabel) : TreeConstants.MAGIC_LABEL_ID);
        return processorFunction;
    }
}
