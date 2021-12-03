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
package com.alibaba.maxgraph.compiler.tree;

import com.alibaba.maxgraph.Message;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.utils.ReflectionUtils;
import com.google.common.collect.Lists;
import net.objecthunter.exp4j.Expression;
import net.objecthunter.exp4j.tokenizer.Token;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MathStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MathTreeNode extends UnaryTreeNode {
    private static final Logger logger = LoggerFactory.getLogger(MathTreeNode.class);
    private static final String CURRENT = "_";
    private List<TreeNode> ringTreeNodeList;
    private List<String> variableList;
    private Expression expression;
    private Token[] tokens;
    private Map<String, Double> constVariableList = createDefaultVariables();

    public MathTreeNode(
            TreeNode input,
            GraphSchema schema,
            List<TreeNode> ringTreeNodeList,
            MathStep.TinkerExpression tinkerExpression) {
        super(input, NodeType.MAP, schema);
        this.ringTreeNodeList = ringTreeNodeList;
        this.variableList = Lists.newArrayList(tinkerExpression.getVariables());
        this.expression = tinkerExpression.getExpression();
        this.tokens = ReflectionUtils.getFieldValue(Expression.class, this.expression, "tokens");
    }

    private static Map<String, Double> createDefaultVariables() {
        Map<String, Double> vars = new HashMap(4);
        vars.put("pi", 3.141592653589793D);
        vars.put("π", 3.141592653589793D);
        vars.put("φ", 1.61803398874D);
        vars.put("e", 2.718281828459045D);
        return vars;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        throw new IllegalArgumentException("unsuport math operator yet");
        //        LogicalSubQueryPlan logicalSubQueryPlan = new
        // LogicalSubQueryPlan(treeNodeLabelManager, contextManager);
        //        LogicalVertex sourceVertex = getInputNode().getOutputVertex();
        //        logicalSubQueryPlan.addLogicalVertex(sourceVertex);
        //
        //        ProcessorFunction dedupFunction = new
        // ProcessorFunction(QueryFlowOuterClass.OperatorType.DEDUP);
        //        LogicalVertex dedupVertex = new LogicalUnaryVertex(vertexIdManager.getId(),
        // dedupFunction, false, sourceVertex);
        //        logicalSubQueryPlan.addLogicalVertex(dedupVertex);
        //        logicalSubQueryPlan.addLogicalEdge(sourceVertex, dedupVertex, new LogicalEdge());
        //
        //        int ringIndex = 0;
        //        LogicalVertex lastJoinVertex = sourceVertex;
        //        Map<String, Integer> variableLabelList = Maps.newHashMap();
        //        for (String variableName : variableList) {
        //            TreeNode ringTreeNode = ringTreeNodeList.get(ringIndex %
        // ringTreeNodeList.size());
        //            ringIndex++;
        //            if (StringUtils.equals(variableName, CURRENT)) {
        //                continue;
        //            }
        //            if (constVariableList.containsKey(variableName)) {
        //                continue;
        //            }
        //            Map<String, Integer> labelIndexList =
        // treeNodeLabelManager.getLabelIndexList();
        //            if (!labelIndexList.containsKey(variableName)) {
        //                throw new RuntimeException("Cant found label " + variableName);
        //            }
        //            ProcessorFunction selectOneFunction = createSelectOneFunction(variableName,
        // Pop.last, labelIndexList);
        //            LogicalVertex selectVertex = new LogicalUnaryVertex(
        //                    vertexIdManager.getId(),
        //                    selectOneFunction,
        //                    true,
        //                    dedupVertex);
        //            logicalSubQueryPlan.addLogicalVertex(selectVertex);
        //            logicalSubQueryPlan.addLogicalEdge(dedupVertex, selectVertex, new
        // LogicalEdge());
        //            if (ringTreeNode instanceof ByKeyTreeNode) {
        //                ((ByKeyTreeNode) ringTreeNode).setJoinValueFlag(false);
        //            }
        //
        //            TreeNode currentTreeNode = CompilerUtils.getSourceTreeNode(ringTreeNode);
        //            boolean propLocalFlag = selectVertex.isPropLocalFlag();
        //
        //            BaseTreeNode sourceTreeNode = null;
        //            boolean sourcePropLocalFlag = false;
        //            while (currentTreeNode != null) {
        //                if (currentTreeNode instanceof SourceDelegateNode) {
        //                    currentTreeNode.setFinishVertex(selectVertex, treeNodeLabelManager);
        //                    sourceTreeNode = BaseTreeNode.class.cast(currentTreeNode);
        //                    sourcePropLocalFlag = sourceTreeNode.isPropLocalFlag();
        //                    sourceTreeNode.setPropLocalFlag(propLocalFlag);
        //                } else {
        //                    if (currentTreeNode instanceof ByKeyTreeNode) {
        //                        if (null == ByKeyTreeNode.class.cast(currentTreeNode).getByKey())
        // {
        //                            String key =
        // treeNodeLabelManager.createSysLabelStart(sourceVertex, "val");
        //                            ByKeyTreeNode.class.cast(currentTreeNode).addByKey(key,
        // sourceVertex, getInputNode().getOutputValueType());
        //                        }
        //                    }
        //
        // logicalSubQueryPlan.mergeLogicalQueryPlan(currentTreeNode.buildLogicalQueryPlan(contextManager, vertexIdManager, treeNodeLabelManager));
        //                }
        //                currentTreeNode = currentTreeNode.getOutputNode();
        //            }
        //            sourceTreeNode.setPropLocalFlag(sourcePropLocalFlag);
        //
        //            List<LogicalVertex> outputVertexList =
        // logicalSubQueryPlan.getOutputVertexList();
        //            outputVertexList.remove(lastJoinVertex);
        //            checkArgument(outputVertexList.size() == 1);
        //            LogicalVertex outputVertex = outputVertexList.get(0);
        //            QueryFlowOuterClass.OperatorType joinType =
        // CompilerUtils.parseJoinOperatorType(ringTreeNode);
        //            String joinLabel = treeNodeLabelManager.createSysLabelStart("val");
        //            int joinLabelIndex = treeNodeLabelManager.getLabelIndex(joinLabel);
        //            variableLabelList.put(variableName, joinLabelIndex);
        //
        //            ProcessorFunction joinFunction = new ProcessorFunction(joinType,
        // Message.Value.newBuilder().setIntValue(joinLabelIndex));
        //            LogicalBinaryVertex logicalBinaryVertex = new
        // LogicalBinaryVertex(vertexIdManager.getId(), joinFunction, false, lastJoinVertex,
        // outputVertex);
        //            logicalSubQueryPlan.addLogicalVertex(logicalBinaryVertex);
        //            logicalSubQueryPlan.addLogicalEdge(lastJoinVertex, logicalBinaryVertex, new
        // LogicalEdge());
        //            logicalSubQueryPlan.addLogicalEdge(outputVertex, logicalBinaryVertex, new
        // LogicalEdge());
        //            lastJoinVertex = logicalBinaryVertex;
        //        }
        //
        //        QueryFlowOuterClass.MathArgumentProto.Builder mathArgumentBuilder =
        // QueryFlowOuterClass.MathArgumentProto.newBuilder();
        //        for (Token token : tokens) {
        //            QueryFlowOuterClass.MathTokenType tokenType =
        // QueryFlowOuterClass.MathTokenType.forNumber(token.getType());
        //            checkNotNull(tokenType, "parse token type from " + token.toString() + "
        // fail");
        //            QueryFlowOuterClass.MathOperatorProto.Builder operatorBuilder =
        // QueryFlowOuterClass.MathOperatorProto.newBuilder()
        //                    .setMathTokenType(tokenType);
        //            switch (tokenType) {
        //                case TOKEN_ERROR: {
        //                    String errorMessage = "Invalid math token type for " +
        // token.toString();
        //                    logger.error(errorMessage);
        //                    throw new RuntimeException(errorMessage);
        //                }
        //                case TOKEN_NUMBER: {
        //                    NumberToken numberToken = NumberToken.class.cast(token);
        //                    QueryFlowOuterClass.MathOperatorValueProto operatorValueProto =
        // QueryFlowOuterClass.MathOperatorValueProto.newBuilder()
        //                            .setDoubleValue(numberToken.getValue()).build();
        //                    operatorBuilder.setOperatorValue(operatorValueProto);
        //                    break;
        //                }
        //                case TOKEN_FUNCTION: {
        //                    FunctionToken functionToken = FunctionToken.class.cast(token);
        //                    QueryFlowOuterClass.MathFunctionType functionType =
        // QueryFlowOuterClass.MathFunctionType.valueOf("FUNC_"
        //                            +
        // StringUtils.upperCase(functionToken.getFunction().getName()));
        //                    int argumentNumber = functionToken.getFunction().getNumArguments();
        //
        // operatorBuilder.setOperatorValue(QueryFlowOuterClass.MathOperatorValueProto.newBuilder()
        //
        // .setFuncArg(QueryFlowOuterClass.MathFunctionArgumentProto.newBuilder()
        //                                    .setFunctionType(functionType)
        //                                    .setArgumentNumber(argumentNumber)
        //                                    .build())
        //                            .build());
        //                    break;
        //                }
        //                case TOKEN_OPERATOR: {
        //                    OperatorToken operatorToken = OperatorToken.class.cast(token);
        //                    Operator operator = operatorToken.getOperator();
        //                    int numOperands = operator.getNumOperands();
        //                    String symbol = operator.getSymbol();
        //                    int opTokenIndex = -1;
        //                    if (StringUtils.equals(symbol, "+")) {
        //                        if (numOperands != 1) {
        //                            opTokenIndex = 0;
        //                        } else {
        //                            opTokenIndex = 7;
        //                        }
        //                    } else if (StringUtils.equals(symbol, "-")) {
        //                        if (numOperands != 1) {
        //                            opTokenIndex = 1;
        //                        } else {
        //                            opTokenIndex = 6;
        //                        }
        //                    } else if (StringUtils.equals(symbol, "*")) {
        //                        opTokenIndex = 2;
        //                    } else if (StringUtils.equals(symbol, "/")) {
        //                        opTokenIndex = 3;
        //                    } else if (StringUtils.equals(symbol, "^")) {
        //                        opTokenIndex = 4;
        //                    } else if (StringUtils.equals(symbol, "%")) {
        //                        opTokenIndex = 5;
        //                    }
        //                    QueryFlowOuterClass.MathOperatorTokenType opTokenType =
        // QueryFlowOuterClass.MathOperatorTokenType.forNumber(opTokenIndex);
        //
        // operatorBuilder.setOperatorValue(QueryFlowOuterClass.MathOperatorValueProto.newBuilder()
        //
        // .setOpTokenArg(QueryFlowOuterClass.MathOpTokenArgumentProto.newBuilder()
        //                                    .setOpTokenType(opTokenType)
        //                                    .setNumOperands(numOperands)
        //                                    .build())
        //                            .build());
        //                    break;
        //                }
        //                case TOKEN_VARIABLE: {
        //                    VariableToken variableToken = VariableToken.class.cast(token);
        //                    String variableName = variableToken.getName();
        //                    if (constVariableList.containsKey(variableName)) {
        //
        // operatorBuilder.setMathTokenType(QueryFlowOuterClass.MathTokenType.TOKEN_NUMBER)
        //
        // .setOperatorValue(QueryFlowOuterClass.MathOperatorValueProto.newBuilder()
        //
        // .setDoubleValue(constVariableList.get(variableName))
        //                                        .build());
        //                    } else if (!StringUtils.equals(variableName, CURRENT)) {
        //
        // operatorBuilder.setOperatorValue(QueryFlowOuterClass.MathOperatorValueProto.newBuilder()
        //                                .setIntValue(variableLabelList.get(variableName))
        //                                .build());
        //                    }
        //                    break;
        //                }
        //                case TOKEN_SEPARATOR:
        //                case TOKEN_PARENTHESES_OPEN:
        //                case TOKEN_PARENTHESES_CLOSE: {
        //                    break;
        //                }
        //                default: {
        //                    break;
        //                }
        //            }
        //            mathArgumentBuilder.addMathTokenList(operatorBuilder);
        //        }
        //
        //        ProcessorFunction mathFunction = new ProcessorFunction(
        //                QueryFlowOuterClass.OperatorType.MATH,
        //                Message.Value.newBuilder()
        //                        .setPayload(mathArgumentBuilder.build().toByteString()));
        //        LogicalUnaryVertex mathVertex = new LogicalUnaryVertex(vertexIdManager.getId(),
        // mathFunction, false, lastJoinVertex);
        //        logicalSubQueryPlan.addLogicalVertex(mathVertex);
        //        logicalSubQueryPlan.addLogicalEdge(lastJoinVertex, mathVertex, new LogicalEdge());
        //
        //        addUsedLabelAndRequirement(mathVertex, treeNodeLabelManager);
        //        setFinishVertex(mathVertex, treeNodeLabelManager);
        //
        //        return logicalSubQueryPlan;
    }

    @Override
    public ValueType getOutputValueType() {
        return new ValueValueType(Message.VariantType.VT_DOUBLE);
    }
}
