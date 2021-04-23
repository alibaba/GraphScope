package com.alibaba.maxgraph.v2.frontend.compiler.optimizer;

import com.alibaba.maxgraph.proto.v2.BinaryOperator;
import com.alibaba.maxgraph.proto.v2.OperatorBase;
import com.alibaba.maxgraph.proto.v2.SourceOperator;
import com.alibaba.maxgraph.proto.v2.UnaryOperator;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.TreeNodeLabelManager;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

public class OperatorListManager {
    private static final String PREPARE_PREFIX = "@";

    private final TreeNodeLabelManager labelManager;

    private Map<Integer, OperatorBase.Builder> operatorBaseList = Maps.newHashMap();
    private SourceOperator.Builder sourceBuilder = null;
    private OperatorBase.Builder sourceBaseBuilder = null;
    private Map<Integer, UnaryOperator.Builder> unaryOperatorList = Maps.newHashMap();
    private Map<Integer, BinaryOperator.Builder> binaryOperatorList = Maps.newHashMap();
    private int argumentIndex = 1;

    public OperatorListManager(TreeNodeLabelManager labelManager) {
        this.labelManager = labelManager;
    }

    public void addOperator(int id, UnaryOperator.Builder builder) {
        unaryOperatorList.put(id, builder);
    }

    public void addOperator(int id, BinaryOperator.Builder builder) {
        binaryOperatorList.put(id, builder);
    }

    public void addOperatorBase(int id, OperatorBase.Builder builder) {
        operatorBaseList.put(id, builder);
    }

    public void setSource(SourceOperator.Builder builder, OperatorBase.Builder sourceBase) {
        Preconditions.checkArgument(sourceBuilder == null);
        sourceBuilder = builder;
        sourceBaseBuilder = sourceBase;
    }

    public OperatorBase.Builder getOperatorBase(int id) {
        return operatorBaseList.get(id);
    }

    public Map<Integer, UnaryOperator.Builder> getUnaryOperatorList() {
        return unaryOperatorList;
    }

    public Map<Integer, BinaryOperator.Builder> getBinaryOperatorList() {
        return binaryOperatorList;
    }

    public SourceOperator.Builder getSourceOperator() {
        return sourceBuilder;
    }

    public OperatorBase.Builder getSourceBase() {
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

    public TreeNodeLabelManager getLabelManager() {
        return labelManager;
    }
}
