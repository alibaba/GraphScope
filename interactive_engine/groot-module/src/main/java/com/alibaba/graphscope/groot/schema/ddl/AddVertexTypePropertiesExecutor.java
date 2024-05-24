package com.alibaba.graphscope.groot.schema.ddl;

import com.alibaba.graphscope.groot.common.schema.wrapper.GraphDef;
import com.alibaba.graphscope.groot.common.schema.wrapper.TypeDef;
import com.alibaba.graphscope.groot.operation.Operation;
import com.alibaba.graphscope.groot.operation.ddl.AddVertexTypePropertiesOperation;

public class AddVertexTypePropertiesExecutor extends AbstractAddTypePropertiesExecutor{

    @Override
    protected Operation makeOperation(int partition, long version, TypeDef typeDef, GraphDef newGraphDef) {
        return new AddVertexTypePropertiesOperation(
                partition, version, typeDef, newGraphDef.getTableIdx()
        );
    }
}
