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
package com.alibaba.maxgraph.compiler.tree;

import com.alibaba.maxgraph.Message;
import com.alibaba.maxgraph.common.util.SchemaUtils;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.api.schema.DataType;
import com.alibaba.maxgraph.compiler.tree.addition.JoinZeroNode;
import com.alibaba.maxgraph.compiler.tree.source.EstimateCountTreeNode;
import com.alibaba.maxgraph.compiler.tree.source.SourceCreateGraphTreeNode;
import com.alibaba.maxgraph.compiler.utils.TreeNodeUtils;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.aggregate.CustomAggregationListTraversal;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.branch.CustomCaseWhenFunction;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.branch.CustomWhenThenFunction;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.map.MapPropFillFunction;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.map.RangeSumFunction;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.program.VertexRatioProgram;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.CustomPredicate;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.ListKeyPredicate;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.ListPredicate;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.PredicateType;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.RegexKeyPredicate;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.RegexPredicate;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.StringKeyPredicate;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.StringPredicate;
import com.alibaba.maxgraph.compiler.tree.value.ListValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueValueType;
import com.alibaba.maxgraph.compiler.tree.value.VarietyValueType;
import com.alibaba.maxgraph.compiler.tree.value.VertexValueType;
import com.alibaba.maxgraph.compiler.optimizer.OptimizeConfig;
import com.alibaba.maxgraph.compiler.strategy.traversal.MaxGraphFilterRankingStrategy;
import com.alibaba.maxgraph.compiler.tree.addition.CountFlagNode;
import com.alibaba.maxgraph.compiler.tree.addition.PropertyNode;
import com.alibaba.maxgraph.compiler.tree.addition.SampleNode;
import com.alibaba.maxgraph.compiler.tree.source.SourceDelegateNode;
import com.alibaba.maxgraph.compiler.tree.source.SourceEdgeTreeNode;
import com.alibaba.maxgraph.compiler.tree.source.SourceTreeNode;
import com.alibaba.maxgraph.compiler.tree.source.SourceVertexTreeNode;
import com.alibaba.maxgraph.compiler.utils.CompilerUtils;
import com.alibaba.maxgraph.compiler.utils.ReflectionUtils;
import com.alibaba.maxgraph.tinkerpop.steps.AllPathStep;
import com.alibaba.maxgraph.tinkerpop.steps.ConnectedComponentsStep;
import com.alibaba.maxgraph.tinkerpop.steps.CreateGraphStep;
import com.alibaba.maxgraph.tinkerpop.steps.CustomVertexProgramStep;
import com.alibaba.maxgraph.tinkerpop.steps.EdgeVertexWithByStep;
import com.alibaba.maxgraph.tinkerpop.steps.EstimateCountStep;
import com.alibaba.maxgraph.tinkerpop.steps.HitsStep;
import com.alibaba.maxgraph.tinkerpop.steps.HitsVertexProgramStep;
import com.alibaba.maxgraph.tinkerpop.steps.LabelPropagationStep;
import com.alibaba.maxgraph.tinkerpop.steps.LpaVertexProgramStep;
import com.alibaba.maxgraph.tinkerpop.steps.MaxGraphStep;
import com.alibaba.maxgraph.tinkerpop.steps.OutputStep;
import com.alibaba.maxgraph.tinkerpop.steps.OutputVineyardStep;
import com.alibaba.maxgraph.tinkerpop.steps.PageRankStep;
import com.alibaba.maxgraph.tinkerpop.steps.ShortestPathStep;
import com.alibaba.maxgraph.tinkerpop.steps.VertexByModulatingStep;
import com.alibaba.maxgraph.tinkerpop.steps.VertexWithByStep;
import com.alibaba.maxgraph.tinkerpop.strategies.MxGraphStepStrategy;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ConnectedComponentVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PageRankVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ShortestPathVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PeerPressureVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.BranchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.ChooseStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AndStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.ConnectiveStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.LambdaFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NotStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.SampleGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.PathFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WherePredicateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectCapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StoreStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ComputerAwareStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.ProfileStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.AdjacentToIncidentStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.FilterRankingStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.IncidentToAdjacentStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.LazyBarrierStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.RepeatUnrollStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.StandardVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.PropertyType;
import org.apache.tinkerpop.gremlin.structure.T;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Syntax tree builder
 */
public class TreeBuilder {
    private static final Logger logger = LoggerFactory.getLogger(TreeBuilder.class);

    private GraphSchema schema;
    private OptimizeConfig optimizeConfig;
    // for compatible, should be removed later
    private TreeNodeLabelManager treeNodeLabelManager;
    private boolean rootPathFlag = true;
    private Set<String> storeSubgraphKeyList = Sets.newHashSet();
    private boolean disableBarrierOptimizer = false;
    private boolean lambdaEnableFlag = false;

    // query config
    private Map<String, Object> queryConfig = Maps.newHashMap();

    public static TreeBuilder newTreeBuilder(GraphSchema schema, OptimizeConfig optimizeConfig, boolean lambdaEnableFlag) {
        return new TreeBuilder(schema, optimizeConfig, lambdaEnableFlag);
    }

    private TreeBuilder(GraphSchema schema, OptimizeConfig optimizeConfig, boolean lambdaEnableFlag) {
        this.schema = schema;
        this.optimizeConfig = optimizeConfig;
        this.treeNodeLabelManager = TreeNodeLabelManager.createLabelManager();
        this.lambdaEnableFlag = lambdaEnableFlag;
    }

    public void setDisableBarrierOptimizer(boolean disableBarrierOptimizer) {
        this.disableBarrierOptimizer = disableBarrierOptimizer;
    }

    public <S, E> TreeManager build(GraphTraversal<S, E> traversal) {
        checkNotNull(traversal, "The 'traversal' argument must be a non-null instance of MaxGraph.");

        GraphTraversal.Admin admin = traversal.asAdmin();
        TreeNode treeNode = travelTraversalAdmin(admin, null);
        if (!admin.getSideEffects().isEmpty() && !admin.getSideEffects().keys().equals(storeSubgraphKeyList)) {
            throw new UnsupportedOperationException("Not support query with SideEffects");
        }
        return new TreeManager(
                treeNode,
                schema,
                treeNodeLabelManager,
                new MapConfiguration(this.queryConfig));
    }

    private <S, E> TreeNode travelTraversalAdmin(Traversal.Admin<S, E> admin, TreeNode parent) {
        // Reuse optimizations from TinkerPop if related.
        if (!admin.isLocked()) {
            List<Step> originalStepList = admin.getSteps();
            List<Step> replaceStepList = Lists.newArrayList();
            for (Step step : originalStepList) {
                if (step instanceof EdgeVertexWithByStep &&
                        null == ((EdgeVertexWithByStep) step).getFunction()) {
                    replaceStepList.add(step);
                }
            }
            for (Step step : replaceStepList) {
                TraversalHelper.replaceStep(step, ((EdgeVertexWithByStep) step).getEdgeVertexStep(), admin);
            }
            admin.getStrategies().removeStrategies(
                    ProfileStrategy.class,
                    MxGraphStepStrategy.class,
                    FilterRankingStrategy.class,
                    StandardVerificationStrategy.class,
                    IncidentToAdjacentStrategy.class,
                    AdjacentToIncidentStrategy.class);
            if (this.disableBarrierOptimizer) {
                admin.getStrategies().removeStrategies(
                        RepeatUnrollStrategy.class,
                        LazyBarrierStrategy.class);
            }
            admin.getStrategies().addStrategies(MaxGraphFilterRankingStrategy.instance());
            admin.applyStrategies();
        }

        Step step = admin.getStartStep();
        if (step.equals(EmptyStep.instance())) {
            TreeNode treeNode = travelTraversalDirectly(admin, parent);
            if (rootPathFlag) {
                UnaryTreeNode.class.cast(treeNode).setPathFlag(true);
            }
            return treeNode;
        }

        TreeNode treeNode = parent;
        while (!step.equals(EmptyStep.instance())) {
            treeNode = visitStep(step, treeNode);
            processPathFlag(treeNode, step);
            Set<String> labelList = step.getLabels();
            for (String label : labelList) {
                if (!StringUtils.startsWith(label, "~gremlin.")) {
                    this.treeNodeLabelManager.addUserTreeNodeLabel(label, treeNode);
                }
            }
            step = step.getNextStep();
        }
        return treeNode;
    }

    private <S, E> TreeNode travelTraversalDirectly(Traversal.Admin<S, E> admin, TreeNode parent) {
        if (admin instanceof TokenTraversal) {
            return new TokenTreeNode(parent, schema, TokenTraversal.class.cast(admin).getToken());
        } else if (admin instanceof ElementValueTraversal) {
            ElementValueTraversal elementValueTraversal = ElementValueTraversal.class.cast(admin);
            String propKey = elementValueTraversal.getPropertyKey();
            TreeNode bypassTreeNode = null;
            Traversal.Admin<?, ?> bypassTraversal = ReflectionUtils.getFieldValue(AbstractLambdaTraversal.class, elementValueTraversal, "bypassTraversal");
            if (null != bypassTraversal) {
                bypassTreeNode = travelTraversalAdmin(bypassTraversal, new SourceDelegateNode(parent, schema));
            }
            return new ElementValueTreeNode(parent, propKey, bypassTreeNode, schema);
        } else if (admin instanceof ColumnTraversal) {
            ColumnTraversal columnTraversal = ColumnTraversal.class.cast(admin);
            return new ColumnTreeNode(parent, schema, columnTraversal.getColumn());
        } else if (admin instanceof IdentityTraversal) {
            return parent;
        } else if (admin instanceof TrueTraversal) {
            return parent;
        } else if (admin instanceof ConstantTraversal) {
            return new ConstantTreeNode(parent, schema, admin.next());
        } else if (admin instanceof CustomAggregationListTraversal) {
            CustomAggregationListTraversal customAggregationListTraversal = CustomAggregationListTraversal.class.cast(admin);
            List<TreeNode> aggNodeList = Lists.newArrayList();
            boolean saveFlag = this.rootPathFlag;
            this.rootPathFlag = false;
            List<Traversal<?, ?>> traversalList = customAggregationListTraversal.getTraversalList();
            for (Traversal<?, ?> aggTraversal : traversalList) {
                aggNodeList.add(travelTraversalAdmin(aggTraversal.asAdmin(), new SourceDelegateNode(parent, schema)));
            }
            this.rootPathFlag = saveFlag;
            return new AggregationListTreeNode(parent, schema, customAggregationListTraversal.getNameList(), aggNodeList);
        } else {
            throw new IllegalArgumentException("Not deal with direct traversal => " + admin);
        }
    }

    private void processPathFlag(TreeNode treeNode, Step step) {
        if (rootPathFlag) {
            if (step instanceof FilterStep
                    || step instanceof GraphStep
                    || treeNode.getNodeType() == NodeType.SOURCE
                    || treeNode.getNodeType() == NodeType.FILTER
                    || treeNode.getNodeType() == NodeType.UNION) {
                return;
            }
            BaseTreeNode.class.cast(treeNode).setPathFlag(true);
        }
    }

    private TreeNode visitStep(Step step, TreeNode prev) {
        try {
            switch (TreeNodeStep.valueOf(step.getClass().getSimpleName())) {
                case MaxGraphStep:
                case GraphStep: {
                    if (null != prev) {
                        throw new IllegalArgumentException("Not support multiple source operator");
                    }

                    return visitGraphStep((GraphStep) step);
                }
                case TraversalVertexProgramStep: {
                    if (null != prev) {
                        return prev;
                    }
                    return visitGraphStep((TraversalVertexProgramStep) step);
                }
                case EstimateCountStep:
                    return visitEstimateCountStep((EstimateCountStep) step);
                case CreateGraphStep:
                    return visitCreateGraphStep((CreateGraphStep) step);
                case VertexStep:
                    return visitVertexStep((VertexStep) step, prev);
                case PathStep:
                    return visitPathStep((PathStep) step, prev);
                case EdgeVertexStep:
                    return visitEdgeVertexStep((EdgeVertexStep) step, prev);
                case HasStep:
                    return visitHasStep((HasStep) step, prev);
                case SelectOneStep:
                    return visitSelectOneStep((SelectOneStep) step, prev);
                case NoOpBarrierStep:
                    return visitNoOpBarrierStep((NoOpBarrierStep) step, prev);
                case CountGlobalStep:
                    return visitCountGlobalStep((CountGlobalStep) step, prev);
                case FoldStep:
                    return visitFoldStep((FoldStep) step, prev);
                case SumGlobalStep:
                    return visitSumGlobalStep((SumGlobalStep) step, prev);
                case MaxGlobalStep:
                    return visitMaxGlobalStep((MaxGlobalStep) step, prev);
                case MinGlobalStep:
                    return visitMinGlobalStep((MinGlobalStep) step, prev);
                case SelectStep:
                    return visitSelectStep((SelectStep) step, prev);
                case TraversalFilterStep:
                    return visitTraversalFilterStep((TraversalFilterStep) step, prev);
                case WherePredicateStep:
                    return visitWherePredicateStep((WherePredicateStep) step, prev);
                case DedupGlobalStep:
                    return visitDedupGlobalStep((DedupGlobalStep) step, prev);
                case OrderGlobalStep:
                    return visitOrderGlobalStep((OrderGlobalStep) step, prev);
                case IdStep:
                    return visitIdStep((IdStep) step, prev);
                case RangeGlobalStep:
                    return visitRangeGlobalStep((RangeGlobalStep) step, prev);
                case CountLocalStep:
                    return visitCountLocalStep((CountLocalStep) step, prev);
                case UnfoldStep:
                    return visitUnfoldStep((UnfoldStep) step, prev);
                case GroupCountStep:
                    return visitGroupCountStep((GroupCountStep) step, prev);
                case IsStep:
                    return visitIsStep((IsStep) step, prev);
                case PropertiesStep:
                    return visitPropertiesStep((PropertiesStep) step, prev);
                case PropertyMapStep:
                    return visitPropertyMapStep((PropertyMapStep) step, prev);
                case SimplePathStep:
                    return visitSimplePathStep((PathFilterStep) step, prev);
                case PathFilterStep:
                    return visitSimplePathStep((PathFilterStep) step, prev);
                case RangeLocalStep:
                    return visitRangeLocalStep((RangeLocalStep) step, prev);
                case OrderLocalStep:
                    return visitOrderLocalStep((OrderLocalStep) step, prev);
                case LambdaFilterStep:
                    return visitLambdaFilterStep((LambdaFilterStep) step, prev);
                case LambdaMapStep:
                    return visitLambdaMapStep((LambdaMapStep) step, prev);
                case LambdaFlatMapStep:
                    return visitLambdaFlatMap((LambdaFlatMapStep) step, prev);
                case NotStep:
                    return visitNotStep((NotStep) step, prev);
                case EdgeOtherVertexStep:
                    return visitEdgeOtherVertexStep((EdgeOtherVertexStep) step, prev);
                case UnionStep:
                    return visitUnionStep((UnionStep) step, prev);
                case AndStep:
                    return visitAndStep((AndStep) step, prev);
                case TraversalMapStep:
                    return visitTraversalMapStep((TraversalMapStep) step, prev);
                case ConstantStep:
                    return visitConstantStep((ConstantStep) step, prev);
                case GroupStep:
                    return visitGroupStep((GroupStep) step, prev);
                case LabelStep:
                    return visitLabelStep((LabelStep) step, prev);
                case PropertyValueStep:
                    return visitPropertyValueStep((PropertyValueStep) step, prev);
                case PropertyKeyStep:
                    return visitPropertyKeyStep((PropertyKeyStep) step, prev);
                case RepeatStep:
                    return visitRepeatStep((RepeatStep) step, prev);
                case RepeatEndStep:
                    return visitRepeatEndStep((RepeatStep.RepeatEndStep) step, prev);
                case EndStep:
                    return visitEndStep((ComputerAwareStep.EndStep) step, prev);
                case SampleGlobalStep:
                    return visitSampleGlobalStep((SampleGlobalStep) step, prev);
                case VertexByModulatingStep:
                    return visitVertexByModulatingStep((VertexByModulatingStep) step, prev);
                case OrStep:
                    return visitOrStep((OrStep) step, prev);
                case CustomVertexProgramStep:
                    return visitCustomVertexProgramStep((CustomVertexProgramStep) step, prev);
                case ChooseStep:
                    return visitChooseStep((ChooseStep) step, prev);
                case BranchStep:
                    return visitBranchStep((BranchStep) step, prev);
                case VertexWithByStep:
                    return visitVertexWithByStep((VertexWithByStep) step, prev);
                case StoreStep:
                    return visitStoreStep((StoreStep) step, prev);
                case LoopsStep:
                    return visitLoopsStep((LoopsStep) step, prev);
                case EdgeVertexWithByStep:
                    return visitEdgeVertexWithByStep((EdgeVertexWithByStep) step, prev);
                case IdentityStep:
                    return visitIdentityStep((IdentityStep) step, prev);
                case SubgraphStep:
                    return visitSubgraphStep((SubgraphStep) step, prev);
                case SideEffectCapStep:
                    return visitSideEffectCapStep((SideEffectCapStep) step, prev);
//                case CacheStep:
//                    return visitCacheStep((CacheStep) step, prev);
                case ConnectedComponentsStep:
                    return visitConnectedComponentsStep((ConnectedComponentsStep) step, prev);
                case ConnectedComponentVertexProgramStep:
                    return visitConnectedComponentsStep((ConnectedComponentVertexProgramStep) step, prev);
                case LabelPropagationStep:
                    return visitLabelPropagationStep((LabelPropagationStep) step, prev);
                case LpaVertexProgramStep:
                    return visitLabelPropagationStep((LpaVertexProgramStep) step, prev);
                case PageRankStep:
                    return visitPageRankStep((PageRankStep) step, prev);
                case PageRankVertexProgramStep:
                    return visitPageRankStep((PageRankVertexProgramStep) step, prev);
                case HitsStep:
                    return visitHitsStep((HitsStep) step, prev);
                case HitsVertexProgramStep:
                    return visitHitsStep((HitsVertexProgramStep) step, prev);
                case AllPathStep:
                    return visitAllPathStep((AllPathStep) step, prev);
                case ShortestPathStep:
                    return visitShortestPathStep((ShortestPathStep) step, prev);
                case ShortestPathVertexProgramStep:
                    return visitShortestPathStep((ShortestPathVertexProgramStep) step, prev);
                case PeerPressureVertexProgramStep:
                    return visitPeerPressureVertexProgramStep((PeerPressureVertexProgramStep) step, prev);
                case OutputStep:
                    return visitOutputStep((OutputStep) step, prev);
                case MathStep:
                    return visitMathStep((MathStep) step, prev);
//                case SackStep:
//                    return visitSackStep((SackStep) step, prev);
//                case SackValueStep:
//                    return visitSackValueStep((SackValueStep) step, prev);
                case TraversalFlatMapStep:
                    return visitFlatMapStep((TraversalFlatMapStep) step, prev);
                case HasNextStep: {
                    return prev;
                }
                case ComputerResultStep: {
                    return prev;
                }
                case OutputVineyardStep: {
                    return visitOutputVineyardStep((OutputVineyardStep) step, prev);
                }
                default:
                    throw new NotImplementedException(step.toString());
            }
        } catch (NotImplementedException e) {
            throw new UnsupportedOperationException("Support invalid for step of " + step.getClass().getSimpleName(), e);
        } catch (Exception e) {
            throw new IllegalArgumentException("Something error in " + step.getClass().getSimpleName() + " operator", e);
        }
    }

    private TreeNode visitOutputVineyardStep(OutputVineyardStep step, TreeNode prev) {
        String graphName = step.getGraphName();
        if (prev instanceof SubgraphTreeNode) {
            ((SubgraphTreeNode) prev).enableVertexFlag();
        } else {
            throw new IllegalArgumentException("The previous operator before outputVineyard must be subgraph('graphName')");
        }
        return new OutputVineyardTreeNode(prev, schema, graphName);
    }

    private TreeNode visitCreateGraphStep(CreateGraphStep step) {
        return new SourceCreateGraphTreeNode(this.schema, step.getGraphName(), step.getConfiguration());
    }

    private TreeNode visitFlatMapStep(TraversalFlatMapStep step, TreeNode prev) {
        Traversal.Admin<?, ?> flatMapTraversal = ReflectionUtils.getFieldValue(TraversalFlatMapStep.class, step, "flatMapTraversal");

        boolean saveFlag = rootPathFlag;
        rootPathFlag = false;
        TreeNode flatMapNode = travelTraversalAdmin(flatMapTraversal, new SourceDelegateNode(prev, schema));
        rootPathFlag = saveFlag;

        return new TraversalFlatMapTreeNode(prev, schema, flatMapNode);
    }

    private TreeNode visitMathStep(MathStep step, TreeNode prev) {
        MathStep.TinkerExpression tinkerExpression = ReflectionUtils.getFieldValue(MathStep.class, step, "expression");
        TraversalRing<?, Number> traversalRing = ReflectionUtils.getFieldValue(MathStep.class, step, "traversalRing");
        List<TreeNode> ringTreeNodeList = Lists.newArrayList();
        traversalRing.getTraversals().forEach(v -> ringTreeNodeList.add(travelTraversalAdmin(v, new SourceDelegateNode(prev, schema))));

        return new MathTreeNode(prev, schema, ringTreeNodeList, tinkerExpression);
    }

//    private TreeNode visitCacheStep(CacheStep step, TreeNode prev) {
//        return new CacheTreeNode(prev, Sets.newHashSet(step.getPropertyKeys()), schema);
//    }

    private TreeNode visitSideEffectCapStep(SideEffectCapStep step, TreeNode prev) {
        if (prev instanceof SubgraphTreeNode) {
            return prev;
        } else {
            throw new UnsupportedOperationException("Not support side effect yet.");
        }
    }

    private TreeNode visitSubgraphStep(SubgraphStep step, TreeNode prev) {
        storeSubgraphKeyList.add(step.getSideEffectKey());
        return new SubgraphTreeNode(prev, schema);
    }

    private TreeNode visitIdentityStep(IdentityStep step, TreeNode prev) {
        return prev;
    }

    private TreeNode visitEdgeVertexWithByStep(EdgeVertexWithByStep step, TreeNode prev) {
        Function function = step.getFunction();
        if (null == function) {
            return visitEdgeVertexStep(step.getEdgeVertexStep(), prev);
        }
        if (function instanceof VertexRatioProgram) {
            VertexRatioProgram vertexRatioProgram = VertexRatioProgram.class.cast(function);
            return new RatioEdgeVertexTreeNode(
                    prev,
                    schema,
                    step.getDirection(),
                    vertexRatioProgram.getPredicate());
        } else {
            throw new UnsupportedOperationException();
        }
    }

    private TreeNode visitLoopsStep(LoopsStep step, TreeNode prev) {
        return prev;
    }

    private TreeNode visitStoreStep(StoreStep step, TreeNode prev) {
        String sideEffectKey = checkNotNull(step.getSideEffectKey());
        Traversal.Admin<?, ?> storeTraversal = ReflectionUtils.getFieldValue(StoreStep.class, step, "storeTraversal");
        storeSubgraphKeyList.add(sideEffectKey);

        boolean saveFlag = rootPathFlag;
        rootPathFlag = false;
        TreeNode storeTreeNode = null;
        if (null != storeTraversal) {
            storeTreeNode = travelTraversalAdmin(storeTraversal, new SourceDelegateNode(prev, schema));
        }
        rootPathFlag = saveFlag;
        return new StoreTreeNode(prev, schema, sideEffectKey, storeTreeNode);
    }

    private TreeNode visitVertexWithByStep(VertexWithByStep step, TreeNode prev) {
        if (null == step.getFunction()) {
            return visitVertexStep(step, prev);
        } else {
            Function function = step.getFunction();
            if (function instanceof VertexRatioProgram) {
                VertexRatioProgram vertexRatioProgram = VertexRatioProgram.class.cast(function);
                return new RatioVertexTreeNode(
                        prev,
                        schema,
                        step.getDirection(),
                        vertexRatioProgram.getPredicate(),
                        null == step.getEdgeLabels() ? Lists.newArrayList() : Lists.newArrayList(step.getEdgeLabels()));
            } else {
                throw new UnsupportedOperationException(function.toString());
            }
        }
    }

    private TreeNode visitBranchStep(BranchStep step, TreeNode prev) {
        Traversal.Admin<?, ?> branchTraversal = ReflectionUtils.getFieldValue(BranchStep.class, step, "branchTraversal");
        Map<TraversalOptionParent.Pick, List<Traversal.Admin<?, ?>>> traversalPickOptions =
                ReflectionUtils.getFieldValue(BranchStep.class, step, "traversalPickOptions");
        List<org.javatuples.Pair<Traversal.Admin, Traversal.Admin<?, ?>>> traversalOptions =
                ReflectionUtils.getFieldValue(BranchStep.class, step, "traversalOptions");
        checkNotNull(branchTraversal, "branch traversal can't be null");
        checkArgument(!traversalPickOptions.isEmpty() || !traversalOptions.isEmpty(),
                "traversal options can't be empty");

        Map<Object, List<Traversal.Admin<?, ?>>> traversalAllOptions = new HashMap<>();
        traversalAllOptions.putAll(traversalPickOptions);
        traversalOptions.forEach(pair -> {
            Traversal.Admin left = pair.getValue0();
            if (left instanceof PredicateTraversal.Admin) {
                PredicateTraversal predicateTraversal = (PredicateTraversal) left;
                P p = ReflectionUtils.getFieldValue(PredicateTraversal.class, predicateTraversal, "predicate");
                traversalAllOptions.computeIfAbsent(p.getValue(), k -> Lists.newArrayList()).add(pair.getValue1());
            }
        });

        boolean saveFlag = rootPathFlag;
        rootPathFlag = false;
        TreeNode branchTreeNode = travelTraversalAdmin(branchTraversal, new SourceDelegateNode(prev, schema));
        rootPathFlag = saveFlag;

        TreeNode noneTreeNode = null, anyTreeNode = null;
        Map<Object, List<TreeNode>> branchOptionList = Maps.newHashMap();
        BranchTreeNode branchOptionTreeNode = new BranchTreeNode(prev, schema, branchTreeNode);
        for (Map.Entry<Object, List<Traversal.Admin<?, ?>>> entry : traversalAllOptions.entrySet()) {
            if (entry.getKey() == TraversalOptionParent.Pick.none) {
                checkArgument(entry.getValue().size() == 1);
                noneTreeNode = travelTraversalAdmin(entry.getValue().get(0), new SourceDelegateNode(prev, schema));
            } else if (entry.getKey() == TraversalOptionParent.Pick.any) {
                checkArgument(entry.getValue().size() == 1);
                anyTreeNode = travelTraversalAdmin(entry.getValue().get(0), new SourceDelegateNode(prev, schema));
            } else {
                List<TreeNode> optionTreeNodeList = Lists.newArrayList();
                for (Traversal.Admin<?, ?> v : entry.getValue()) {
                    TreeNode optionTreeNode = travelTraversalAdmin(v, new SourceDelegateNode(prev, schema));
                    optionTreeNodeList.add(optionTreeNode);
                }
                branchOptionList.put(entry.getKey(), optionTreeNodeList);
            }
        }
        branchOptionTreeNode.setNoneTreeNode(noneTreeNode);
        branchOptionTreeNode.setAnyTreeNode(anyTreeNode);
        branchOptionTreeNode.setOptionTreeNodeList(branchOptionList);

        return branchOptionTreeNode;
    }

    private TreeNode visitChooseStep(ChooseStep step, TreeNode prev) {
        List<org.javatuples.Pair<Traversal.Admin, Traversal.Admin<?, ?>>> traversalOptions =
                ReflectionUtils.getFieldValue(BranchStep.class, step, "traversalOptions");
        Traversal.Admin<?, ?> trueOptionTraversal, falseOptionTraversal;
        if (traversalOptions.size() == 2
                && (trueOptionTraversal = getTraversalOption(true, traversalOptions)) != null
                && (falseOptionTraversal = getTraversalOption(false, traversalOptions)) != null) {

            boolean saveFlag = this.rootPathFlag;
            this.rootPathFlag = false;
            Traversal.Admin<?, ?> branchTraversal = ReflectionUtils.getFieldValue(BranchStep.class, step, "branchTraversal");
            TreeNode branchNode = branchTraversal == null ? null : travelTraversalAdmin(branchTraversal, new SourceDelegateNode(prev, schema));
            this.rootPathFlag = saveFlag;

            TreeNode trueOptionNode = travelTraversalAdmin(trueOptionTraversal, new SourceDelegateNode(prev, schema));
            TreeNode falseOptionNode = travelTraversalAdmin(falseOptionTraversal, new SourceDelegateNode(prev, schema));

            return new OptionalTreeNode(prev, schema, branchNode, trueOptionNode, falseOptionNode);
        } else {
            throw new UnsupportedOperationException("Not support choose yet.");
        }
    }

    private Traversal.Admin<?, ?> getTraversalOption(boolean predicate, List<org.javatuples.Pair<Traversal.Admin,
            Traversal.Admin<?, ?>>> traversalOptions) {
        for (int i = 0; i < 2; ++i) {
            org.javatuples.Pair<Traversal.Admin, Traversal.Admin<?, ?>> pair = traversalOptions.get(i);
            Traversal.Admin left = pair.getValue0();
            if (left instanceof PredicateTraversal.Admin) {
                PredicateTraversal predicateTraversal = (PredicateTraversal) left;
                P p = ReflectionUtils.getFieldValue(PredicateTraversal.class, predicateTraversal, "predicate");
                if (p.getValue().equals(predicate)) {
                    return pair.getValue1();
                }
            }
        }
        return null;
    }

    private TreeNode visitOrStep(OrStep step, TreeNode prev) {
        List<Traversal.Admin<?, ?>> traversals = ReflectionUtils.getFieldValue(ConnectiveStep.class, step, "traversals");
        List<TreeNode> orTreeNodeList = Lists.newArrayList();

        boolean saveFlag = rootPathFlag;
        rootPathFlag = false;
        traversals.forEach(v -> orTreeNodeList.add(travelTraversalAdmin(v, new SourceDelegateNode(prev, schema))));
        rootPathFlag = saveFlag;

        return new OrTreeNode(orTreeNodeList, prev, schema);
    }

    private TreeNode visitCustomVertexProgramStep(CustomVertexProgramStep step, TreeNode prev) {
        VertexProgram vertexProgram = step.getCustomProgram();
        return new CustomVertexProgramTreeNode(prev, schema, vertexProgram);
    }

    private TreeNode visitConnectedComponentsStep(ConnectedComponentsStep step, TreeNode prev) {
        return new ConnectComponetsTreeNode(prev, schema, step.outPropId, step.iteration);
    }

    private TreeNode visitConnectedComponentsStep(ConnectedComponentVertexProgramStep step, TreeNode prev) {
        return new ConnectComponetsVertexProgramTreeNode(prev, schema, step);
    }

    private TreeNode visitLabelPropagationStep(LabelPropagationStep step, TreeNode prev) {
        return new LabelPropagationTreeNode(prev, schema, step);
    }

    private TreeNode visitLabelPropagationStep(LpaVertexProgramStep step, TreeNode prev) {
        return new LpaVertexProgramTreeNode(prev, schema, step);
    }

    private TreeNode visitPageRankStep(PageRankStep step, TreeNode prev) {
        return new PageRankTreeNode(prev, schema, step.outPropId, step.alpha, step.iteration);
    }

    private TreeNode visitPageRankStep(PageRankVertexProgramStep step, TreeNode prev) {
        return new PageRankVertexProgramTreeNode(prev, schema, step);
    }

    private TreeNode visitHitsStep(HitsStep step, TreeNode prev) {
        return new HitsTreeNode(prev, schema, step.outPropAuthId, step.outPropHubId, step.iteration);
    }

    private TreeNode visitHitsStep(HitsVertexProgramStep step, TreeNode prev) {
        return new HitsVertexProgramTreeNode(prev, schema, step);
    }

    private TreeNode visitAllPathStep(AllPathStep step, TreeNode prev) {
        return new AllPathTreeNode(prev, schema, step.sid, step.tid, step.khop, step.outPropId);
    }

    private TreeNode visitShortestPathStep(ShortestPathStep step, TreeNode prev) {
        return new ShortestPathTreeNode(prev, schema, step);
    }

    private TreeNode visitShortestPathStep(ShortestPathVertexProgramStep step, TreeNode prev) {
        Traversal.Admin<?, ?> targetVertexFilter = step.getLocalChildren().get(0);
        Step targetStep = targetVertexFilter.getSteps().get(0);
        if (targetStep instanceof HasStep) {
            List<HasContainer> targetHasContainerList = ((HasStep) targetStep).getHasContainers();
            List<HasContainer> convertContainerList = targetHasContainerList.stream()
                    .map(v -> convertHasContainer(v.getKey(), v.getPredicate(), prev.getOutputValueType()))
                    .collect(Collectors.toList());
            HasContainer targetHascontainer = convertContainerList.get(0);
            if (prev instanceof SourceTreeNode) {
                List<HasContainer> sourceHasContainerList = ((SourceTreeNode) prev).hasContainerList;
                int idx = sourceHasContainerList.size() - 1;
                if (idx >= 0) {
                    HasContainer sourceHasContainer = sourceHasContainerList.get(idx);
                    if (targetHascontainer.getKey().equals(sourceHasContainer.getKey())) {
                        HasContainer targetContainer = new HasContainer(targetHascontainer.getKey(), P.within(Lists.newArrayList(sourceHasContainer.getPredicate().getValue(), targetHascontainer.getPredicate().getValue())));
                        ((SourceTreeNode) prev).hasContainerList.set(idx, targetContainer);
                    } else {
                        throw new UnsupportedOperationException(targetStep.toString());
                    }
                } else {
                    // do not support target vertex exists without source vertex
                    throw new UnsupportedOperationException(targetStep.toString());
                }
            }
        } else if (targetStep instanceof IdentityStep) {

        } else {
            throw new UnsupportedOperationException(targetStep.toString());
        }
        return new ShortestPathVertexProgramTreeNode(prev, schema, step);
    }

    private TreeNode visitPeerPressureVertexProgramStep(PeerPressureVertexProgramStep step, TreeNode prev) {
        return new PeerPressureVertexProgramTreeNode(prev, schema, step);
    }

    private TreeNode visitOutputStep(OutputStep step, TreeNode prev) {
        return new OutputTreeNode(prev, schema, step.path, step.properties);
    }

    private TreeNode visitVertexByModulatingStep(VertexByModulatingStep step, TreeNode prev) {
        checkNotNull(prev);
        Direction direction = step.getDirection();
        String[] edgeLabels = step.getEdgeLabels();
        SampleGlobalStep sampleGlobalStep = step.getSampleGlobalStep();

        SampleNode sampleNode;
        if (step.returnsVertex()) {
            sampleNode = new VertexTreeNode(prev, direction, edgeLabels, schema);
        } else {
            sampleNode = new EdgeTreeNode(prev, direction, edgeLabels, schema);
        }
        if (null != sampleGlobalStep) {
            int amountToSample = ReflectionUtils.getFieldValue(SampleGlobalStep.class, sampleGlobalStep, "amountToSample");
            Traversal.Admin<?, ?> probabilityTraversal = ReflectionUtils.getFieldValue(SampleGlobalStep.class, sampleGlobalStep, "probabilityTraversal");
            boolean saveFlag = rootPathFlag;
            rootPathFlag = false;
            String probabilityProperty = null;
            TreeNode probabilityTreeNode = travelTraversalAdmin(probabilityTraversal, new SourceDelegateNode((TreeNode) sampleNode, schema));
            if (probabilityTreeNode instanceof SourceTreeNode) {
                probabilityProperty = "";
            } else if ((UnaryTreeNode.class.cast(probabilityTreeNode).getInputNode() instanceof SourceTreeNode &&
                    (probabilityTreeNode instanceof PropertyNode))) {
                probabilityProperty = PropertyNode.class.cast(probabilityTreeNode).getPropKeyList().iterator().next();
            } else {
                throw new IllegalArgumentException("Only support sample by property here.");
            }
            rootPathFlag = saveFlag;
            sampleNode.setSample(amountToSample, probabilityProperty);
        }
        return (TreeNode) sampleNode;
    }

    private TreeNode visitSampleGlobalStep(SampleGlobalStep step, TreeNode prev) {
        int amountToSample = ReflectionUtils.getFieldValue(SampleGlobalStep.class, step, "amountToSample");
        if (amountToSample != 1) {
            throw new IllegalArgumentException("Only support sample 1");
        }
        Traversal.Admin<?, ?> probabilityTraversal = ReflectionUtils.getFieldValue(SampleGlobalStep.class, step, "probabilityTraversal");
        if (!(probabilityTraversal instanceof ConstantTraversal)) {
            throw new IllegalArgumentException("Not support sample by probability yet");
        }
        ConstantTraversal constantTraversal = (ConstantTraversal) probabilityTraversal;
        if (!(constantTraversal.next() instanceof Double)) {
            throw new IllegalArgumentException("Not support sample by probability yet");
        }
        Double constantValue = (Double) constantTraversal.next();
        if (constantValue != 1.0) {
            throw new IllegalArgumentException("Not support sample by probability yet");
        }
        return new SampleGlobalTreeNode(prev, schema, amountToSample);
    }

    private TreeNode visitEndStep(ComputerAwareStep.EndStep step, TreeNode prev) {
        return prev;
    }

    private TreeNode visitRepeatEndStep(RepeatStep.RepeatEndStep step, TreeNode prev) {
        return prev;
    }

    private TreeNode visitRepeatStep(RepeatStep step, TreeNode prev) {
        Traversal.Admin<?, ?> repeatTraversal = ReflectionUtils.getFieldValue(RepeatStep.class, step, "repeatTraversal");
        Traversal.Admin<?, ?> untilTraversal = ReflectionUtils.getFieldValue(RepeatStep.class, step, "untilTraversal");
        Traversal.Admin<?, ?> emitTraversal = ReflectionUtils.getFieldValue(RepeatStep.class, step, "emitTraversal");

        String repeatTraversalString = repeatTraversal.toString();
        String untilTraversalString = untilTraversal == null ? "" : untilTraversal.toString();
        String emitTraversalString = emitTraversal == null ? "" : emitTraversal.toString();
        if (StringUtils.contains(repeatTraversalString, "RepeatStep") ||
                StringUtils.contains(untilTraversalString, "RepeatStep") ||
                StringUtils.contains(emitTraversalString, "RepeatStep")) {
            throw new UnsupportedOperationException("Not support nest repeat");
        }

        boolean untilFirst = ReflectionUtils.getFieldValue(RepeatStep.class, step, "untilFirst");
        boolean emitFirst = ReflectionUtils.getFieldValue(RepeatStep.class, step, "emitFirst");

        RepeatTreeNode repeatTreeNode = new RepeatTreeNode(prev, schema, this.queryConfig);
        SourceDelegateNode sourceDelegateNode = new SourceDelegateNode(prev, schema);
        sourceDelegateNode.enableRepeatFlag();
        repeatTreeNode.setRepeatBodyTreeNode(travelTraversalAdmin(repeatTraversal, sourceDelegateNode));

        boolean saveFlag = rootPathFlag;
        this.rootPathFlag = false;
        if (null != untilTraversal) {
            if (untilTraversal instanceof LoopTraversal) {
                repeatTreeNode.setMaxLoopTimes(LoopTraversal.class.cast(untilTraversal).getMaxLoops());
            } else {
                if (untilFirst) {
                    repeatTreeNode.setUntilFirstTreeNode(travelTraversalAdmin(untilTraversal, new SourceDelegateNode(prev, schema)));
                }
                Step finalEndStep = untilTraversal.getEndStep();
                if (finalEndStep instanceof OrStep) {
                    OrStep orStep = OrStep.class.cast(finalEndStep);
                    List<Traversal.Admin<?, ?>> traversals = ReflectionUtils.getFieldValue(ConnectiveStep.class, orStep, "traversals");
                    checkArgument(traversals.size() == 2, "Only support two condition in until yet");
                    Traversal.Admin<?, ?> firstTraversal = traversals.get(0);
                    Traversal.Admin<?, ?> secondTraversal = traversals.get(1);
                    Traversal.Admin<?, ?> loopTraversal;
                    if (firstTraversal.getSteps().get(0) instanceof LoopsStep &&
                            !(secondTraversal.getSteps().get(0) instanceof LoopsStep)) {
                        repeatTreeNode.setUntilTreeNode(travelTraversalAdmin(secondTraversal, new SourceDelegateNode(prev, schema)));
                        loopTraversal = firstTraversal;
                    } else if (secondTraversal.getSteps().get(0) instanceof LoopsStep &&
                            !(firstTraversal.getSteps().get(0) instanceof LoopsStep)) {
                        repeatTreeNode.setUntilTreeNode(travelTraversalAdmin(firstTraversal, new SourceDelegateNode(prev, schema)));
                        loopTraversal = secondTraversal;
                    } else {
                        throw new UnsupportedOperationException("There's no looop condition in until");
                    }

                    List<Step> loopStepList = loopTraversal.getSteps();
                    checkArgument(loopStepList.size() == 2 && loopStepList.get(1) instanceof IsStep,
                            "Only support loops().is(gt(loop count)) yet.");
                    IsStep isStep = (IsStep) loopStepList.get(1);
                    P predicate = isStep.getPredicate();
                    BiPredicate biPredicate = predicate.getBiPredicate();
                    checkArgument(biPredicate == Compare.gt || biPredicate == Compare.gte || biPredicate == Compare.eq,
                            "Only support loops().is(gt/eq/gte(loop count)) yet.");
                    long loopCount = Long.parseLong(predicate.getValue().toString());
                    if (biPredicate == Compare.gt) {
                        loopCount += 1;
                    }
                    checkArgument(loopCount > 0, "Invalid loop count must > 0");
                    repeatTreeNode.setMaxLoopTimes(loopCount);
                } else if (finalEndStep instanceof AndStep) {
                    throw new IllegalArgumentException("Not support and operator in until yet.");
                } else {
                    repeatTreeNode.setUntilTreeNode(travelTraversalAdmin(untilTraversal, new SourceDelegateNode(prev, schema)));
                }
            }
        }
        if (null != emitTraversal) {
            if (emitFirst) {
                repeatTreeNode.setEmitFirstTreeNode(travelTraversalAdmin(emitTraversal, new SourceDelegateNode(prev, schema)));
            }
            repeatTreeNode.setEmitTreeNode(travelTraversalAdmin(emitTraversal, new SourceDelegateNode(prev, schema)));
        }
        rootPathFlag = saveFlag;

        return repeatTreeNode;
    }

    private TreeNode visitLambdaMapStep(LambdaMapStep step, TreeNode prev) {
        Function functionObj = step.getMapFunction();
        Function mapFunction;
        if (functionObj instanceof FunctionTraverser) {
            mapFunction = ReflectionUtils.getFieldValue(FunctionTraverser.class, functionObj, "function");
        } else {
            mapFunction = functionObj;
        }

        if (mapFunction instanceof CustomCaseWhenFunction) {
            CustomCaseWhenFunction customCaseWhenFunction = CustomCaseWhenFunction.class.cast(mapFunction);
            return processCaseWhenFunction(prev, customCaseWhenFunction);
        } else if (mapFunction instanceof CustomAggregationListTraversal) {
            CustomAggregationListTraversal customAggregationListTraversal = CustomAggregationListTraversal.class.cast(mapFunction);
            List<Traversal<?, ?>> aggregateTraversalList = customAggregationListTraversal.getTraversalList();
            List<TreeNode> aggregateNodeList = Lists.newArrayList();
            boolean saveFlag = rootPathFlag;
            this.rootPathFlag = false;
            for (Traversal<?, ?> traversal : aggregateTraversalList) {
                aggregateNodeList.add(travelTraversalAdmin(traversal.asAdmin(), new SourceDelegateNode(prev, schema)));
            }
            this.rootPathFlag = saveFlag;
            return new AggregationListTreeNode(prev, schema, customAggregationListTraversal.getNameList(), aggregateNodeList);
        } else if (mapFunction instanceof MapPropFillFunction || mapFunction instanceof RangeSumFunction) {
            return new LambdaMapTreeNode(prev, schema, mapFunction, null);
        } else {
            if (this.lambdaEnableFlag) {
                return new LambdaMapTreeNode(prev, schema, mapFunction, buildLambdaIndex(step));
            } else {
                throw new UnsupportedOperationException("Not support lambda map yet");
            }
        }
    }

    private TreeNode processCaseWhenFunction(TreeNode prev, CustomCaseWhenFunction customCaseWhenFunction) {
        Traversal<?, ?> caseTraversal = customCaseWhenFunction.getCaseTraversal();
        List<CustomWhenThenFunction> whenThenFunctionList = customCaseWhenFunction.getWhenThenFunctionList();
        Traversal<?, ?> elseEndTraversal = customCaseWhenFunction.getElseEndTraversal();

        boolean saveFlag = rootPathFlag;
        this.rootPathFlag = false;

        TreeNode caseTreeNode = travelTraversalAdmin(caseTraversal.asAdmin(), new SourceDelegateNode(prev, schema));
        List<Pair<TreeNode, TreeNode>> whenThenNodeList = Lists.newArrayList();
        for (CustomWhenThenFunction whenThenFunction : whenThenFunctionList) {
            Pair<TreeNode, TreeNode> whenThenPair = Pair.of(
                    travelTraversalAdmin(whenThenFunction.getWhenPredicate().asAdmin(), new SourceDelegateNode(caseTreeNode, schema)),
                    travelTraversalAdmin(whenThenFunction.getThenTraversal().asAdmin(), new SourceDelegateNode(caseTreeNode, schema)));
            whenThenNodeList.add(whenThenPair);
        }
        TreeNode elseEndTreeNode = elseEndTraversal == null ? null : travelTraversalAdmin(elseEndTraversal.asAdmin(), new SourceDelegateNode(caseTreeNode, schema));

        this.rootPathFlag = saveFlag;
        return new CaseWhenTreeNode(prev, schema, caseTreeNode, whenThenNodeList, elseEndTreeNode);
    }

    private TreeNode visitPropertyKeyStep(PropertyKeyStep step, TreeNode prev) {
        return new PropertyKeyValueTreeNode(prev, schema, Message.PropKeyValueType.PROP_KEY_TYPE);
    }

    private TreeNode visitPropertyValueStep(PropertyValueStep step, TreeNode prev) {
        return new PropertyKeyValueTreeNode(prev, schema, Message.PropKeyValueType.PROP_VALUE_TYPE);
    }

    private TreeNode visitLabelStep(LabelStep step, TreeNode prev) {
        return new TokenTreeNode(prev, schema, T.label);
    }

    private TreeNode visitGroupStep(GroupStep step, TreeNode prev) {
        Traversal.Admin<?, ?> keyTraversal = ReflectionUtils.getFieldValue(GroupStep.class, step, "keyTraversal");
        Traversal.Admin<?, ?> valueTraversal = ReflectionUtils.getFieldValue(GroupStep.class, step, "valueTraversal");
        if (null != keyTraversal &&
                (StringUtils.contains(keyTraversal.toString(), "GroupStep") ||
                        StringUtils.contains(keyTraversal.toString(), "GroupCountStep"))) {
            throw new IllegalArgumentException("Not support group by (group or group count) in key");
        }
        if (null != valueTraversal && (StringUtils.contains(valueTraversal.toString(), "GroupStep") ||
                StringUtils.contains(valueTraversal.toString(), "GroupCountStep"))) {
            throw new IllegalArgumentException("Not support group by (group or group count) in value");
        }

        GroupTreeNode groupTreeNode = new GroupTreeNode(prev, schema);
        boolean saveFlag = rootPathFlag;
        rootPathFlag = false;
        if (null != keyTraversal) {
            groupTreeNode.setKeyTreeNode(travelTraversalAdmin(keyTraversal, new SourceDelegateNode(prev, schema)));
        }
        if (null != valueTraversal) {
            groupTreeNode.setValueTreeNode(travelTraversalAdmin(valueTraversal, new SourceDelegateNode(prev, schema)));
        }
        rootPathFlag = saveFlag;

        return groupTreeNode;
    }

    private TreeNode visitConstantStep(ConstantStep step, TreeNode prev) {
        return new ConstantTreeNode(prev, schema, ReflectionUtils.getFieldValue(ConstantStep.class, step, "constant"));
    }

    private TreeNode visitTraversalMapStep(TraversalMapStep step, TreeNode prev) {
        TreeNode mapTreeNode = travelTraversalAdmin(ReflectionUtils.getFieldValue(TraversalMapStep.class, step, "mapTraversal"), new SourceDelegateNode(prev, schema));
        List<TreeNode> mapTreeNodeList = TreeNodeUtils.buildTreeNodeListFromLeaf(mapTreeNode);
        for (TreeNode treeNode : mapTreeNodeList) {
            if (treeNode.getNodeType() == NodeType.AGGREGATE || treeNode instanceof DedupGlobalTreeNode) {
                throw new UnsupportedOperationException("Not support traversal in map");
            }
        }
        return new TraversalMapTreeNode(prev, schema, mapTreeNode);
    }

    private TreeNode visitAndStep(AndStep step, TreeNode prev) {
        List<Traversal.Admin<?, ?>> traversals = ReflectionUtils.getFieldValue(ConnectiveStep.class, step, "traversals");
        List<TreeNode> andTreeNodeList = Lists.newArrayList();
        boolean saveFlag = rootPathFlag;
        rootPathFlag = false;
        traversals.forEach(v -> {
            TreeNode treeNode = travelTraversalAdmin(v, new SourceDelegateNode(prev, schema));
            if (treeNode instanceof HasTreeNode) {
                TreeNode hasInputNode = ((HasTreeNode) treeNode).getInputNode();
                if (hasInputNode instanceof SourceVertexTreeNode ||
                        hasInputNode instanceof SourceEdgeTreeNode ||
                        hasInputNode instanceof EdgeTreeNode) {
                    prev.addHasContainerList(((HasTreeNode) treeNode).getHasContainerList());
                } else {
                    andTreeNodeList.add(treeNode);
                }
            } else {
                andTreeNodeList.add(treeNode);
            }
        });
        rootPathFlag = saveFlag;

        if (andTreeNodeList.isEmpty()) {
            return prev;
        } else {
            AndTreeNode andTreeNode = new AndTreeNode(prev, schema);
            andTreeNode.getAndTreeNodeList().addAll(andTreeNodeList);

            return andTreeNode;
        }
    }

    private TreeNode visitUnionStep(UnionStep step, TreeNode prev) {
        List<Traversal.Admin<?, ?>> unionTraversalList = step.getGlobalChildren();
        List<TreeNode> unionTreeNodeList = Lists.newArrayList();

        unionTraversalList.forEach(v -> unionTreeNodeList.add(travelTraversalAdmin(v, new SourceDelegateNode(prev, schema))));

        UnionTreeNode unionTreeNode = new UnionTreeNode(prev, schema, unionTreeNodeList);
        return unionTreeNode;
    }

    private TreeNode visitEdgeOtherVertexStep(EdgeOtherVertexStep step, TreeNode prev) {
        return new EdgeOtherVertexTreeNode(prev, schema);
    }

    private TreeNode visitNotStep(NotStep step, TreeNode prev) {
        Traversal.Admin<?, ?> notTraversal = ReflectionUtils.getFieldValue(NotStep.class, step, "notTraversal");
        TreeNode notTreeNode;

        boolean saveFlag = rootPathFlag;
        rootPathFlag = false;
        notTreeNode = travelTraversalAdmin(notTraversal, new SourceDelegateNode(prev, schema));
        rootPathFlag = saveFlag;

        return new NotTreeNode(prev, schema, notTreeNode);
    }

    private HasContainer convertPredicateToHasContainer(Predicate<Traversal<?, ?>> predicate, ValueType valueType) {
        if (predicate instanceof CustomPredicate) {
            switch (((CustomPredicate) predicate).getPredicateType()) {
                case REGEX: {
                    RegexPredicate regexPredicate = RegexPredicate.class.cast(predicate);
                    if (predicate instanceof RegexKeyPredicate) {
                        RegexKeyPredicate regexKeyPredicate = RegexKeyPredicate.class.cast(regexPredicate);
                        String key = regexKeyPredicate.getKey();
                        return convertHasContainer(key, regexKeyPredicate, valueType);
                    } else {
                        return convertHasContainer("", regexPredicate, valueType);
                    }
                }
                case STRING: {
                    StringPredicate stringPredicate = StringPredicate.class.cast(predicate);
                    if (predicate instanceof StringKeyPredicate) {
                        String key = StringKeyPredicate.class.cast(predicate).getKey();
                        return convertHasContainer(key, stringPredicate, valueType);
                    } else {
                        return convertHasContainer("", stringPredicate, valueType);
                    }

                }
                case LIST: {
                    ListPredicate listPredicate = ListPredicate.class.cast(predicate);
                    if (listPredicate instanceof ListKeyPredicate) {
                        String key = ListKeyPredicate.class.cast(listPredicate).getKey();
                        return convertHasContainer(key, listPredicate, valueType);
                    } else {
                        return convertHasContainer("", listPredicate, valueType);
                    }
                }
                default: {
                    throw new UnsupportedOperationException("Only support custom predicate in lambda filter yet.");
                }
            }
        } else if (predicate instanceof ConnectiveP) {
            throw new UnsupportedOperationException("Not support or/and here");
        } else {
            return convertHasContainer("", predicate, valueType);
        }
    }

    private TreeNode visitLambdaFilterStep(LambdaFilterStep step, TreeNode prev) {
        if (step.getPredicate() instanceof P) {
            Predicate predicate = step.getPredicate();
            List<HasContainer> hasContainerList = Lists.newArrayList();
            hasContainerList.add(convertPredicateToHasContainer(predicate, prev.getOutputValueType()));

            if (prev instanceof SourceVertexTreeNode ||
                    prev instanceof SourceEdgeTreeNode ||
                    prev instanceof EdgeTreeNode) {
                prev.addHasContainerList(hasContainerList);
                return prev;
            } else {
                return new HasTreeNode(prev, hasContainerList, schema);
            }

        } else {
            if (this.lambdaEnableFlag) {
                return new LambdaFilterTreeNode(prev, schema, buildLambdaIndex(step));
            } else {
                throw new UnsupportedOperationException("Not support lambda filter yet");
            }
        }
    }

    private TreeNode visitLambdaFlatMap(LambdaFlatMapStep step, TreeNode prev) {
        if (this.lambdaEnableFlag) {
            return new LambdaFlatMapTreeNode(prev, schema, buildLambdaIndex(step));
        } else {
            throw new UnsupportedOperationException("Not support lambda flat map yet");
        }
    }

    private String buildLambdaIndex(Step step) {
        StringBuilder lambdaIndex = new StringBuilder();
        Step current = step;
        lambdaIndex.append(String.valueOf(current.getTraversal().getSteps().indexOf(current)));

        while (!(current.getTraversal().getParent() instanceof EmptyStep)) {
            Traversal.Admin currentTraversal = current.getTraversal();
            TraversalParent parent = currentTraversal.getParent();
            lambdaIndex.append('#');
            if (!parent.getGlobalChildren().isEmpty() && -1 != parent.getGlobalChildren().get(0).getSteps().indexOf(current)) {
                lambdaIndex.append('G');
            } else {
                lambdaIndex.append('L');
            }
            current = parent.asStep();
            lambdaIndex.append(String.valueOf(current.getTraversal().getSteps().indexOf(current)));
        }

        return lambdaIndex.reverse().toString();
    }

    private TreeNode visitOrderLocalStep(OrderLocalStep step, TreeNode prev) {
        List<org.javatuples.Pair<Traversal.Admin<?, ?>, Comparator<?>>> comparatorList = step.getComparators();
        List<Pair<TreeNode, Order>> orderTreeNodeList = Lists.newArrayList();

        boolean saveFlag = rootPathFlag;
        rootPathFlag = false;
        comparatorList.forEach(v -> orderTreeNodeList.add(Pair.of(travelTraversalAdmin(v.getValue0(), new SourceDelegateNode(prev, schema)), Order.class.cast(v.getValue1()))));
        rootPathFlag = saveFlag;

        return new OrderLocalTreeNode(prev, schema, orderTreeNodeList);
    }

    private TreeNode visitRangeLocalStep(RangeLocalStep step, TreeNode prev) {
        long low = ReflectionUtils.getFieldValue(RangeLocalStep.class, step, "low");
        long high = ReflectionUtils.getFieldValue(RangeLocalStep.class, step, "high");
        return new RangeLocalTreeNode(prev, schema, low, high);
    }

    private TreeNode visitSimplePathStep(PathFilterStep step, TreeNode prev) {
        String fromLabel = ReflectionUtils.getFieldValue(PathFilterStep.class, step, "fromLabel");
        String toLabel = ReflectionUtils.getFieldValue(PathFilterStep.class, step, "toLabel");
        TraversalRing<?, ?> traversalRing = ReflectionUtils.getFieldValue(PathFilterStep.class, step, "traversalRing");
        if (StringUtils.isNotEmpty(fromLabel) ||
                StringUtils.isNotEmpty(toLabel) ||
                (null != traversalRing
                        && traversalRing.size() > 0)) {
            throw new IllegalArgumentException("Not support fromLabel/toLabel/traversalRing in path filter step");
        }
        boolean isSimple = ReflectionUtils.getFieldValue(PathFilterStep.class, step, "isSimple");
        Set<String> propKeyList = Sets.newHashSet();
        Set<ValueType> pathValueList = Sets.newHashSet();
        processPathRequirement(prev, propKeyList, pathValueList);

        return new SimplePathTreeNode(prev, schema, isSimple);
    }

    private TreeNode visitPropertyMapStep(PropertyMapStep step, TreeNode prev) {
        Traversal.Admin<?, ?> propertyTraversal = step.getLocalChildren().isEmpty() ? null : (Traversal.Admin<?, ?>) step.getLocalChildren().get(0);
        TraversalRing<?, ?> traversalRing = ReflectionUtils.getFieldValue(PropertyMapStep.class, step, "traversalRing");
        if (null != propertyTraversal || !traversalRing.isEmpty()) {
            throw new UnsupportedOperationException("Not support value map with property traversal or traversal ring");
        }
        String[] propertyKeys = step.getPropertyKeys();
        PropertyType propertyType = step.getReturnType();
        return new PropertyMapTreeNode(prev, schema, propertyKeys, propertyType, step.isIncludeTokens());
    }

    private TreeNode visitPropertiesStep(PropertiesStep step, TreeNode prev) {
        return new PropertiesTreeNode(prev, schema, step.getPropertyKeys(), step.getReturnType());
    }

    private TreeNode visitIsStep(IsStep step, TreeNode prev) {
        P predicate = step.getPredicate();
        Object value = predicate.getValue();
        if (prev instanceof JoinZeroNode) {
            if (Double.parseDouble(value.toString()) > 0.0 && predicate.getBiPredicate() == Compare.gte) {
                ((JoinZeroNode) prev).disableJoinZero();
            } else if (Double.parseDouble(value.toString()) >= 0.0 && predicate.getBiPredicate() == Compare.gt) {
                ((JoinZeroNode) prev).disableJoinZero();
            }
        }
        HasContainer hasContainer = convertHasContainer("", predicate, prev.getOutputValueType());
        return new HasTreeNode(prev, Lists.newArrayList(hasContainer), schema);
    }

    private HasContainer convertHasContainer(String key, Predicate<?> predicate, ValueType inputValueType) {
        if (null == predicate) {
            return new HasContainer(key, null);
        }
        if (predicate instanceof CustomPredicate) {
            if (StringUtils.isEmpty(key)) {
                checkArgument(inputValueType instanceof ListValueType || inputValueType instanceof ValueValueType,
                        "Output value for predicate must be value while current type=>" + inputValueType);
                PredicateType predicateType = ((CustomPredicate) predicate).getPredicateType();
                Message.VariantType variantType = inputValueType instanceof ListValueType ?
                        Message.VariantType.valueOf(ValueValueType.class.cast(((ListValueType) inputValueType).getListValue()).getDataType().name() + "_LIST") :
                        ValueValueType.class.cast(inputValueType).getDataType();
                validPredicateVariantType(variantType, predicateType);
                return new HasContainer(key, CustomPredicate.class.cast(predicate));
            } else {
                Set<DataType> dataTypeSet = SchemaUtils.getPropDataTypeList(key, schema);
                if (dataTypeSet.isEmpty()) {
                    return new HasContainer(key, CustomPredicate.class.cast(predicate));
                } else {
                    if (dataTypeSet.size() > 1) {
                        logger.warn("There's multiple type=>" + dataTypeSet + " for property=>" + key);
                        return new HasContainer(key, CustomPredicate.class.cast(predicate));
                    } else {
                        Message.VariantType variantType = CompilerUtils.parseVariantFromDataType(dataTypeSet.iterator().next());
                        PredicateType predicateType = ((CustomPredicate) predicate).getPredicateType();
                        validPredicateVariantType(variantType, predicateType);
                        return new HasContainer(key, CustomPredicate.class.cast(predicate));
                    }
                }
            }
        } else {
            if (StringUtils.isEmpty(key)) {
                Message.VariantType variantType = ValueValueType.class.cast(inputValueType).getDataType();
                return createContainerFromVariantyType(key, P.class.cast(predicate), variantType);
            } else {
                Set<DataType> dataTypeSet = SchemaUtils.getPropDataTypeList(key, schema);
                if (dataTypeSet.isEmpty()) {
                    return new HasContainer(key, P.class.cast(predicate));
                } else {
                    if (dataTypeSet.size() > 1) {
                        logger.warn("There's multiple type=>" + dataTypeSet + " for property=>" + key);
                        return new HasContainer(key, P.class.cast(predicate));
                    } else {
                        Message.VariantType variantType = CompilerUtils.parseVariantFromDataType(dataTypeSet.iterator().next());
                        return createContainerFromVariantyType(key, P.class.cast(predicate), variantType);
                    }
                }
            }
        }
    }

    private HasContainer createContainerFromVariantyType(String key, P<?> predicate, Message.VariantType variantType) {
        if (Message.VariantType.VT_UNKNOWN == variantType) {
            return new HasContainer(key, predicate);
        }
        if (predicate.getBiPredicate() instanceof Compare) {
            Object value = CompilerUtils.convertValueWithType(predicate.getValue(), variantType);
            P currentPredicate = new P(predicate.getBiPredicate(), value);
            return new HasContainer(key, currentPredicate);
        } else if (predicate.getBiPredicate() instanceof Contains) {
            Object value = CompilerUtils.convertValueWithType(predicate.getValue(), Message.VariantType.valueOf(variantType.name() + "_LIST"));
            P currentPredicate = new P(predicate.getBiPredicate(), value);
            return new HasContainer(key, currentPredicate);
        } else if (predicate instanceof ConnectiveP) {
            ConnectiveP<Object> connectiveP = (ConnectiveP<Object>) predicate;
            convertConnectiveValueType(connectiveP, variantType);
            return new HasContainer(key, predicate);
        } else {
            return new HasContainer(key, predicate);
        }
    }

    private void convertConnectiveValueType(P<Object> predicate, Message.VariantType variantType) {
        if (predicate instanceof ConnectiveP) {
            List<P<Object>> predicateList = ReflectionUtils.getFieldValue(ConnectiveP.class, predicate, "predicates");
            for (P<Object> currentPredicate : predicateList) {
                convertConnectiveValueType(currentPredicate, variantType);
            }
        } else if (!(predicate instanceof CustomPredicate)) {
            Object value = CompilerUtils.convertValueWithType(predicate.getValue(), variantType);
            predicate.setValue(value);
        }
    }

    private void validPredicateVariantType(Message.VariantType variantType, PredicateType predicateType) {
        switch (predicateType) {
            case STRING:
            case REGEX: {
                checkArgument(variantType == Message.VariantType.VT_STRING,
                        "Text and Regex predicate only support string output while current value type=>" + variantType);
                break;
            }
            case LIST: {
                checkArgument(variantType == Message.VariantType.VT_INT_LIST ||
                                variantType == Message.VariantType.VT_LONG_LIST ||
                                variantType == Message.VariantType.VT_STRING_LIST,
                        "List predicate only support list output while current value type=>" + variantType);
                break;
            }
            default: {
                break;
            }
        }
    }

    private TreeNode visitGroupCountStep(GroupCountStep step, TreeNode prev) {
        GroupCountTreeNode groupCountTreeNode = new GroupCountTreeNode(prev, schema);
        Traversal.Admin<?, ?> keyTraversal = step.getLocalChildren().isEmpty() ? null : (Traversal.Admin<?, ?>) step.getLocalChildren().get(0);
        if (null != keyTraversal) {
            boolean saveFlag = rootPathFlag;
            rootPathFlag = false;
            groupCountTreeNode.setKeyTreeNode(travelTraversalAdmin(keyTraversal, new SourceDelegateNode(prev, schema)));
            rootPathFlag = saveFlag;
        }
        return groupCountTreeNode;
    }

    private TreeNode visitUnfoldStep(UnfoldStep step, TreeNode prev) {
        return new UnfoldTreeNode(prev, schema);
    }

    private TreeNode visitCountLocalStep(CountLocalStep step, TreeNode prev) {
        return new CountLocalTreeNode(prev, schema);
    }

    private TreeNode visitRangeGlobalStep(RangeGlobalStep step, TreeNode prev) {
        long low = ReflectionUtils.getFieldValue(RangeGlobalStep.class, step, "low");
        long high = ReflectionUtils.getFieldValue(RangeGlobalStep.class, step, "high");
        boolean bypass = ReflectionUtils.getFieldValue(RangeGlobalStep.class, step, "bypass");
        if (bypass) {
            return prev;
        }
        TreeNode lastRangeNode = prev;
        while (lastRangeNode.getNodeType() == NodeType.MAP) {
            lastRangeNode = UnaryTreeNode.class.cast(lastRangeNode).getInputNode();
        }

        if (lastRangeNode instanceof OrderGlobalTreeNode) {
            lastRangeNode.setRangeLimit(low, high, rootPathFlag);
            return prev;
        } else if (lastRangeNode.getNodeType() == NodeType.FLATMAP
                || lastRangeNode instanceof SourceVertexTreeNode
                || lastRangeNode instanceof SourceEdgeTreeNode) {
            lastRangeNode.setRangeLimit(0, high - low, rootPathFlag);
            long tmplow = low;
            low = 0;
            high = high - tmplow;
        }

        TreeNode outputNode = lastRangeNode.getOutputNode();
        RangeGlobalTreeNode rangeGlobalTreeNode = new RangeGlobalTreeNode(lastRangeNode, schema, low, high);

        if (null == outputNode) {
            return rangeGlobalTreeNode;
        } else {
            UnaryTreeNode.class.cast(outputNode).setInputNode(rangeGlobalTreeNode);
            return prev;
        }
    }

    private TreeNode visitIdStep(IdStep step, TreeNode prev) {
        return new TokenTreeNode(prev, schema, T.id);
    }

    private TreeNode visitOrderGlobalStep(OrderGlobalStep step, TreeNode prev) {
        List<org.javatuples.Pair<Traversal.Admin<?, ?>, Comparator<?>>> comparatorList = step.getComparators();
        List<Pair<TreeNode, Order>> treeNodeOrderList = Lists.newArrayList();

        boolean saveFlag = rootPathFlag;
        rootPathFlag = false;
        SourceDelegateNode sourceDelegateNode = new SourceDelegateNode(prev, schema);
        comparatorList.forEach(v -> treeNodeOrderList.add(Pair.of(travelTraversalAdmin(v.getValue0(), sourceDelegateNode), Order.class.cast(v.getValue1()))));
        rootPathFlag = saveFlag;

        return new OrderGlobalTreeNode(prev, schema, treeNodeOrderList);
    }

    private TreeNode visitDedupGlobalStep(DedupGlobalStep step, TreeNode prev) {
        Set<String> dedupLabelList = step.getScopeKeys();
        Traversal.Admin<?, ?> dedupTraversal = step.getLocalChildren().isEmpty() ? null : (Traversal.Admin<?, ?>) step.getLocalChildren().get(0);
        DedupGlobalTreeNode dedupGlobalTreeNode = new DedupGlobalTreeNode(prev, schema, dedupLabelList);

        if (null != dedupTraversal) {
            boolean saveFlag = rootPathFlag;
            rootPathFlag = false;
            dedupGlobalTreeNode.setDedupTreeNode(travelTraversalAdmin(dedupTraversal, new SourceDelegateNode(dedupGlobalTreeNode, schema)));
            rootPathFlag = saveFlag;
        }
        List<TreeNode> treeNodeList = Lists.reverse(TreeNodeUtils.buildTreeNodeListFromLeaf(dedupGlobalTreeNode));
        TreeNode sourceTreeNode = treeNodeList.get(treeNodeList.size() - 1);
        boolean subQueryNodeFlag = sourceTreeNode instanceof SourceDelegateNode;
        for (TreeNode treeNode : treeNodeList) {
            if (subQueryNodeFlag) {
                treeNode.setSubqueryNode();
            }
            treeNode.enableDedupLocal();
            if (treeNode instanceof SelectTreeNode ||
                    treeNode instanceof SelectOneTreeNode ||
                    (treeNode instanceof WherePredicateTreeNode &&
                            !treeNodeLabelManager.getLabelIndexList().containsKey(((WherePredicateTreeNode) treeNode).getStartKey()))) {
                break;
            }
        }

        return dedupGlobalTreeNode;
    }

    private TreeNode visitWherePredicateStep(WherePredicateStep step, TreeNode prev) {
        Optional<String> startKeyOptional = step.getStartKey();
        Optional<P<?>> predicateOptional = step.getPredicate();
        List<String> selectKeys = ReflectionUtils.getFieldValue(WherePredicateStep.class, step, "selectKeys");
        List<Traversal.Admin<?, ?>> ringTraversalList = step.getLocalChildren();
        String sourceKey = startKeyOptional.isPresent() ? startKeyOptional.get() : null;
        String targetKey = selectKeys.iterator().next();
        WherePredicateTreeNode wherePredicateTreeNode = new WherePredicateTreeNode(
                prev,
                schema,
                predicateOptional.get(),
                sourceKey,
                ringTraversalList.isEmpty());

        boolean saveFlag = rootPathFlag;
        rootPathFlag = false;
        if (!ringTraversalList.isEmpty()) {
            Traversal.Admin<?, ?> sourceAdmin = ringTraversalList.get(0);
            Traversal.Admin<?, ?> targetAdmin = ringTraversalList.get(1 % ringTraversalList.size());
            TreeNode sourceNode = travelTraversalAdmin(sourceAdmin,
                    null == sourceKey ? new SourceDelegateNode(prev, schema) :
                            new SelectOneTreeNode(
                                    new SourceDelegateNode(prev, schema),
                                    sourceKey,
                                    Pop.last,
                                    treeNodeLabelManager.getTreeNodeList(sourceKey),
                                    schema));
            TreeNode targetNode = travelTraversalAdmin(targetAdmin, new SelectOneTreeNode(
                    new SourceDelegateNode(prev, schema),
                    targetKey,
                    Pop.last,
                    treeNodeLabelManager.getTreeNodeList(targetKey),
                    schema));
            wherePredicateTreeNode.setSourceTargetNode(sourceNode, targetNode);
        }
        rootPathFlag = saveFlag;

        return wherePredicateTreeNode;
    }

    private TreeNode visitTraversalFilterStep(TraversalFilterStep step, TreeNode prev) {
        Traversal.Admin<?, ?> filterTraversal = (Traversal.Admin<?, ?>) step.getLocalChildren().get(0);
        TreeNode filterTreeNode;


        boolean saveFlag = rootPathFlag;
        rootPathFlag = false;
        filterTreeNode = travelTraversalAdmin(filterTraversal, new SourceDelegateNode(prev, schema));
        rootPathFlag = saveFlag;
        if (!(filterTreeNode instanceof RangeGlobalTreeNode)) {
            TreeNode currentTreeNode = filterTreeNode;
            boolean addRangeFlag = false;
            while (currentTreeNode instanceof UnaryTreeNode) {
                if (currentTreeNode.getNodeType() == NodeType.FILTER
                        || currentTreeNode.getNodeType() == NodeType.MAP) {
                    currentTreeNode = UnaryTreeNode.class.cast(currentTreeNode).getInputNode();
                    continue;
                }
                if (currentTreeNode.getNodeType() == NodeType.FLATMAP) {
                    addRangeFlag = true;
                }
                break;
            }
            if (addRangeFlag) {
                filterTreeNode = new RangeGlobalTreeNode(filterTreeNode, schema, 0, 1);
            }
        }

        if (filterTreeNode instanceof SourceTreeNode) {
            throw new IllegalArgumentException();
        } else if (UnaryTreeNode.class.cast(filterTreeNode).getInputNode() instanceof SourceTreeNode
                && (filterTreeNode instanceof SelectOneTreeNode || filterTreeNode instanceof PropertyNode)) {
            String key;
            if (filterTreeNode instanceof SelectOneTreeNode) {
                key = SelectOneTreeNode.class.cast(filterTreeNode).getSelectLabel();
            } else {
                key = PropertyNode.class.cast(filterTreeNode).getPropKeyList().iterator().next();
            }
            HasContainer hasContainer = new HasContainer(key, null);
            if (prev instanceof SourceTreeNode && !(prev instanceof SourceDelegateNode)) {
                SourceTreeNode.class.cast(prev).addHasContainer(hasContainer);
                return prev;
            } else {
                return new HasTreeNode(prev, Lists.newArrayList(hasContainer), schema);
            }
        } else {
            TraversalFilterTreeNode traversalFilterTreeNode = new TraversalFilterTreeNode(prev, schema);
            traversalFilterTreeNode.setFilterTreeNode(filterTreeNode);
            return traversalFilterTreeNode;
        }
    }

    private TreeNode visitSelectStep(SelectStep step, TreeNode prev) {
        List<String> selectKeyList = ReflectionUtils.getFieldValue(SelectStep.class, step, "selectKeys");
        Pop pop = step.getPop();
        List<Traversal.Admin<?, ?>> ringTraversalList = step.getLocalChildren();

        if (selectKeyList.size() < 2) {
            throw new IllegalArgumentException("select key size < 2 for select operator");
        }
        Map<String, List<TreeNode>> labelTreeNodeList = Maps.newHashMap();
        selectKeyList.forEach(v -> {
            if (treeNodeLabelManager.getLabelIndexList().containsKey(v)) {
                labelTreeNodeList.put(v, treeNodeLabelManager.getTreeNodeList(v));
            }
        });
        SelectTreeNode selectTreeNode = new SelectTreeNode(prev, selectKeyList, pop, labelTreeNodeList, schema);

        boolean saveFlag = rootPathFlag;
        rootPathFlag = false;
        if (!ringTraversalList.isEmpty()) {
            Set<ValueType> valueTypeList = selectKeyList.stream().map(v -> treeNodeLabelManager.getValueType(v, pop)).collect(Collectors.toSet());
            ValueType selectValueType = valueTypeList.size() > 1 ? new VarietyValueType(valueTypeList) : valueTypeList.iterator().next();
            ringTraversalList.forEach(v -> {
                SourceDelegateNode sourceDelegateNode = new SourceDelegateNode(selectTreeNode, schema);
                sourceDelegateNode.setDelegateOutputValueType(selectValueType);
                selectTreeNode.addTraversalTreeNode(travelTraversalAdmin(v, sourceDelegateNode));
            });
        }
        rootPathFlag = saveFlag;

        return selectTreeNode;
    }

    private TreeNode visitMinGlobalStep(MinGlobalStep step, TreeNode prev) {
        return new MinTreeNode(prev, schema);
    }

    private TreeNode visitMaxGlobalStep(MaxGlobalStep step, TreeNode prev) {
        return new MaxTreeNode(prev, schema);
    }

    private TreeNode visitSumGlobalStep(SumGlobalStep step, TreeNode prev) {
        return new SumTreeNode(prev, schema);
    }

    private TreeNode visitFoldStep(FoldStep step, TreeNode prev) {
        boolean listFold = ReflectionUtils.getFieldValue(FoldStep.class, step, "listFold");
        if (listFold) {
            return new FoldTreeNode(prev, schema);
        } else {
            throw new UnsupportedOperationException("Only support list fold yet.");
        }
    }

    private TreeNode visitCountGlobalStep(CountGlobalStep step, TreeNode prev) {
        if (prev instanceof CountFlagNode && ((CountFlagNode) prev).checkCountOptimize()) {
            CountFlagNode.class.cast(prev).enableCountFlag();
            SumTreeNode sumTreeNode = new SumTreeNode(prev, schema);
            sumTreeNode.enableJoinZero();
            return sumTreeNode;
        } else {
            return new CountGlobalTreeNode(prev, schema);
        }
    }

    private TreeNode visitNoOpBarrierStep(NoOpBarrierStep step, TreeNode prev) {
        return new BarrierTreeNode(prev, step, schema);
    }

    private TreeNode visitSelectOneStep(SelectOneStep step, TreeNode prev) {
        Pop pop = step.getPop();
        String selectLabel = (String) Lists.newArrayList(step.getScopeKeys()).get(0);
        Traversal.Admin<?, ?> selectTraversal = ReflectionUtils.getFieldValue(SelectOneStep.class, step, "selectTraversal");

        SelectOneTreeNode selectOneTreeNode = new SelectOneTreeNode(prev, selectLabel, pop, treeNodeLabelManager.getLabelTreeNodeList(selectLabel), schema);

        boolean saveFlag = rootPathFlag;
        rootPathFlag = false;
        if (null != selectTraversal) {
            selectOneTreeNode.setTraversalTreeNode(travelTraversalAdmin(selectTraversal, new SourceDelegateNode(selectOneTreeNode, schema)));
        }
        rootPathFlag = saveFlag;

        return selectOneTreeNode;
    }

    private TreeNode visitHasStep(HasStep step, TreeNode prev) {
        List<HasContainer> hasContainerList = step.getHasContainers();
        List<HasContainer> convertContainerList = hasContainerList.stream()
                .map(v -> convertHasContainer(v.getKey(), v.getPredicate(), prev.getOutputValueType()))
                .collect(Collectors.toList());
        if (prev instanceof SourceVertexTreeNode
                || prev instanceof SourceEdgeTreeNode
                || prev instanceof EdgeTreeNode) {
            prev.addHasContainerList(convertContainerList);
            return prev;
        }

        return new HasTreeNode(prev, convertContainerList, schema);
    }

    private TreeNode visitEdgeVertexStep(EdgeVertexStep step, TreeNode prev) {
        return new EdgeVertexTreeNode(prev, step.getDirection(), schema);
    }

    private TreeNode visitPathStep(PathStep step, TreeNode prev) {
        String from = ReflectionUtils.getFieldValue(PathStep.class, step, "fromLabel");
        String to = ReflectionUtils.getFieldValue(PathStep.class, step, "toLabel");
        Set<String> keepLabels = ReflectionUtils.getFieldValue(PathStep.class, step, "keepLabels");
        if (StringUtils.isNotEmpty(from) ||
                StringUtils.isNotEmpty(to) ||
                (null != keepLabels &&
                        keepLabels.size() > 0)) {
            throw new UnsupportedOperationException("Not support from/to/keepLabels in path step");
        }

        PathTreeNode pathTreeNode = new PathTreeNode(prev, schema);
        pathTreeNode.addPathRequirement();
        pathTreeNode.getUsedLabelList().addAll(treeNodeLabelManager.getUserLabelList());

        boolean savePathFlag = this.rootPathFlag;
        this.rootPathFlag = false;

        Set<SourceDelegateNode> ringSourceNodeList = Sets.newHashSet();
        List<Traversal.Admin<?, ?>> traversalRingList = step.getLocalChildren();
        List<TreeNode> ringTreeNodeList = Lists.newArrayList();
        for (Traversal.Admin<?, ?> traversalAdmin : traversalRingList) {
            SourceDelegateNode sourceDelegateNode = new SourceDelegateNode(pathTreeNode, schema);
            ringSourceNodeList.add(sourceDelegateNode);
            ringTreeNodeList.add(travelTraversalAdmin(traversalAdmin, sourceDelegateNode));
        }
        pathTreeNode.setRingTreeNodeList(ringTreeNodeList);

        Set<String> propKeyList = pathTreeNode.getOutputPropList();
        Set<ValueType> pathValueList = Sets.newHashSet();
        processPathRequirement(pathTreeNode, propKeyList, pathValueList);
        pathTreeNode.setPathValueList(pathValueList);

        ringSourceNodeList.forEach(v -> {
            if (pathValueList.size() > 1) {
                v.setDelegateOutputValueType(new VarietyValueType(pathValueList));
            } else {
                v.setDelegateOutputValueType(pathValueList.iterator().next());
            }
        });

        if (!propKeyList.isEmpty()) {
            if (pathTreeNode.getInputNode() instanceof PropFillTreeNode) {
                PropFillTreeNode propFillTreeNode = PropFillTreeNode.class.cast(pathTreeNode.getInputNode());
                propFillTreeNode.getPropKeyList().addAll(propKeyList);
            } else {
                PropFillTreeNode propFillTreeNode = new PropFillTreeNode(prev, propKeyList, schema);
                pathTreeNode.setInputNode(propFillTreeNode);
            }
        }

        this.rootPathFlag = savePathFlag;
        return pathTreeNode;
    }

    private void processPathRequirement(TreeNode treeNode, Set<String> propKeyList, Set<ValueType> pathValueList) {
        if (treeNode instanceof SourceDelegateNode) {
            processPathRequirement(SourceDelegateNode.class.cast(treeNode).getDelegate(), propKeyList, pathValueList);
            return;
        }
        if (treeNode instanceof SourceTreeNode || treeNode.getNodeType() == NodeType.AGGREGATE) {
            return;
        }

        UnaryTreeNode unaryTreeNode = UnaryTreeNode.class.cast(treeNode);
        if (treeNode instanceof RepeatTreeNode) {
            RepeatTreeNode repeatTreeNode = RepeatTreeNode.class.cast(treeNode);
            TreeNode repeatBodyTreeNode = repeatTreeNode.getRepeatBodyTreeNode();
            processPathRequirement(repeatBodyTreeNode, propKeyList, pathValueList);
        } else {
            if (treeNode.isPathFlag()) {
                treeNode.addPathRequirement();
                if (treeNode instanceof PathTreeNode) {
                    ((PathTreeNode) treeNode).disablePathDelete();
                }
                ValueType inputValueType = unaryTreeNode.getInputNode().getOutputValueType();
                pathValueList.add(inputValueType);
                if (null != propKeyList && !propKeyList.isEmpty() && inputValueType instanceof VertexValueType) {
                    if (unaryTreeNode.getInputNode() instanceof PropFillTreeNode) {
                        PropFillTreeNode propFillTreeNode = PropFillTreeNode.class.cast(unaryTreeNode.getInputNode());
                        propFillTreeNode.getPropKeyList().addAll(propKeyList);
                    } else {
                        PropFillTreeNode propFillTreeNode = new PropFillTreeNode(null, propKeyList, schema);
                        TreeNode inputTreeNode = unaryTreeNode.getInputNode();
                        unaryTreeNode.setInputNode(propFillTreeNode);
                        propFillTreeNode.setInputNode(inputTreeNode);
                    }
                }

            }
        }
        processPathRequirement(unaryTreeNode.getInputNode(), propKeyList, pathValueList);
    }

    private TreeNode visitVertexStep(VertexStep step, TreeNode parent) {
        checkNotNull(parent);
        Direction direction = step.getDirection();
        String[] edgeLabels = step.getEdgeLabels();

        if (step.returnsVertex()) {
            return new VertexTreeNode(parent, direction, edgeLabels, schema);
        } else {
            return new EdgeTreeNode(parent, direction, edgeLabels, schema);
        }
    }

    private TreeNode visitEstimateCountStep(EstimateCountStep step) {
        TreeNode estimateCountNode = new EstimateCountTreeNode(step.isVertexFlag(), step.getLabelList(), schema);
        return new SumTreeNode(estimateCountNode, schema);
    }

    /**
     * Convert graph step to SourceTreeNode
     *
     * @param step The given GraphStep
     */
    private TreeNode visitGraphStep(GraphStep step) {
        if (step instanceof MaxGraphStep) {
            Map<String, Object> queryConfig = ((MaxGraphStep) step).getQueryConfig();
            if (null != queryConfig) {
                this.queryConfig.putAll(queryConfig);
            }
        }

        Object[] ids = step.getIds();
        SourceTreeNode sourceTreeNode;
        if (step.returnsVertex()) {
            if (null == ids || ids.length == 0) {
                sourceTreeNode = new SourceVertexTreeNode(schema);
            } else {
                sourceTreeNode = new SourceVertexTreeNode(ids, schema);
            }
        } else {
            if (null == ids || ids.length == 0) {
                sourceTreeNode = new SourceEdgeTreeNode(schema);
            } else {
                sourceTreeNode = new SourceEdgeTreeNode(ids, schema);
            }
        }

        Supplier<?> supplier = step.getTraversal().getSideEffects().getSackInitialValue();
        if (null != supplier) {
            sourceTreeNode.setInitialSackValue(supplier.get());
        }

        return sourceTreeNode;
    }

    private TreeNode visitGraphStep(TraversalVertexProgramStep step) {
        Traversal.Admin<?, ?> computerTraversal = step.getGlobalChildren().get(0);
        TreeNode prev = null;
        List<Step> steps = computerTraversal.getSteps();
        for (Step step1 : steps) {
            prev = visitStep(step1, prev);
        }
        return prev;
    }
}
