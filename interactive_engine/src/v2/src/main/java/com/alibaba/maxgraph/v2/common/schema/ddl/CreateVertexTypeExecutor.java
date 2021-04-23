package com.alibaba.maxgraph.v2.common.schema.ddl;

import com.alibaba.maxgraph.v2.common.operation.Operation;
import com.alibaba.maxgraph.v2.common.operation.ddl.CreateVertexTypeOperation;
import com.alibaba.maxgraph.v2.common.schema.GraphDef;
import com.alibaba.maxgraph.v2.common.schema.TypeDef;

public class CreateVertexTypeExecutor extends AbstractCreateTypeExecutor {

    @Override
    protected Operation makeOperation(int partition, long version, TypeDef typeDef, GraphDef newGraphDef) {
        return new CreateVertexTypeOperation(partition, version, typeDef, newGraphDef.getTableIdx());
    }
}
