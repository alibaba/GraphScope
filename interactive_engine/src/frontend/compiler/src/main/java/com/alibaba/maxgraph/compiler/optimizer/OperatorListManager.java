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

import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.compiler.prepare.store.PrepareEntity;
import com.alibaba.maxgraph.compiler.tree.TreeNodeLabelManager;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

public class OperatorListManager {
    private static final String PREPARE_PREFIX = "@";

    private final TreeNodeLabelManager labelManager;

    private Map<Integer, QueryFlowOuterClass.OperatorBase.Builder> operatorBaseList = Maps.newHashMap();
    private QueryFlowOuterClass.SourceOperator.Builder sourceBuilder = null;
    private QueryFlowOuterClass.OperatorBase.Builder sourceBaseBuilder = null;
    private Map<Integer, QueryFlowOuterClass.UnaryOperator.Builder> unaryOperatorList = Maps.newHashMap();
    private Map<Integer, QueryFlowOuterClass.BinaryOperator.Builder> binaryOperatorList = Maps.newHashMap();
    private List<PrepareEntity> prepareEntityList = Lists.newArrayList();
    private int argumentIndex = 1;

    public OperatorListManager(TreeNodeLabelManager labelManager) {
        this.labelManager = labelManager;
    }

    public void addOperator(int id, QueryFlowOuterClass.UnaryOperator.Builder builder) {
        unaryOperatorList.put(id, builder);
    }

    public void addOperator(int id, QueryFlowOuterClass.BinaryOperator.Builder builder) {
        binaryOperatorList.put(id, builder);
    }

    public void addOperatorBase(int id, QueryFlowOuterClass.OperatorBase.Builder builder) {
        operatorBaseList.put(id, builder);
    }

    public void setSource(QueryFlowOuterClass.SourceOperator.Builder builder, QueryFlowOuterClass.OperatorBase.Builder sourceBase) {
        Preconditions.checkArgument(sourceBuilder == null);
        sourceBuilder = builder;
        sourceBaseBuilder = sourceBase;
    }

    public QueryFlowOuterClass.OperatorBase.Builder getOperatorBase(int id) {
        return operatorBaseList.get(id);
    }

    public Map<Integer, QueryFlowOuterClass.UnaryOperator.Builder> getUnaryOperatorList() {
        return unaryOperatorList;
    }

    public Map<Integer, QueryFlowOuterClass.BinaryOperator.Builder> getBinaryOperatorList() {
        return binaryOperatorList;
    }

    public QueryFlowOuterClass.SourceOperator.Builder getSourceOperator() {
        return sourceBuilder;
    }

    public QueryFlowOuterClass.OperatorBase.Builder getSourceBase() {
        return sourceBaseBuilder;
    }

    public int getAndIncrementArgumentIndex() {
        int currArgumentId = argumentIndex;
        this.argumentIndex++;
        return currArgumentId;
    }

    public static boolean isPrepareValue(String value) {
        return StringUtils.startsWith(value, PREPARE_PREFIX);
    }

    public static int getPrepareValue(String value) {
        return Integer.parseInt(StringUtils.removeStart(value, PREPARE_PREFIX));
    }

    public void addPrepareEntity(PrepareEntity prepareEntity) {
        prepareEntityList.add(prepareEntity);
    }

    public List<PrepareEntity> getPrepareEntityList() {
        return prepareEntityList;
    }

    public TreeNodeLabelManager getLabelManager() {
        return labelManager;
    }
}
