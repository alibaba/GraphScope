package com.alibaba.graphscope.groot.schema.ddl;

import com.alibaba.graphscope.groot.common.schema.wrapper.GraphDef;
import com.alibaba.graphscope.groot.common.schema.wrapper.TypeDef;
import com.alibaba.graphscope.groot.operation.Operation;
import com.alibaba.graphscope.groot.operation.ddl.AddEdgeTypePropertiesOperation;

public class AddEdgeTypePropertiesExecutor extends AbstractAddTypePropertiesExecutor{

    @Override
    protected Operation makeOperation(int partition, long version, TypeDef typeDef, GraphDef newGraphDef) {
        return new AddEdgeTypePropertiesOperation(partition, version, typeDef);
    }
}
