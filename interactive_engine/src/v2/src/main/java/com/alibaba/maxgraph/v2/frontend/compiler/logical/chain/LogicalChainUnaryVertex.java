package com.alibaba.maxgraph.v2.frontend.compiler.logical.chain;

import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalUnaryVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorFunction;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;

import java.util.List;

public class LogicalChainUnaryVertex extends LogicalUnaryVertex {
    private List<LogicalVertex> unaryVertexList = Lists.newArrayList();

    public LogicalChainUnaryVertex(int id, LogicalVertex inputVertex) {
        super(id, new ProcessorFunction(OperatorType.UNARY_CHAIN), inputVertex);
    }

    public void addUnaryVertex(LogicalVertex vertex) {
        this.unaryVertexList.add(vertex);
    }

    @Override
    public ProcessorFunction getProcessorFunction() {
        return new ProcessorUnaryChainFunction(this.unaryVertexList);

    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        unaryVertexList.forEach(v -> stringBuilder.append("_").append(v.toString()));
        stringBuilder.append("_").append(this.getId());

        return StringUtils.removeStart(stringBuilder.toString(), "_");
    }
}
