package com.alibaba.maxgraph.v2.common.schema.ddl;

import com.alibaba.maxgraph.v2.common.operation.Operation;
import com.alibaba.maxgraph.v2.common.operation.ddl.CreateEdgeTypeOperation;
import com.alibaba.maxgraph.v2.common.schema.GraphDef;
import com.alibaba.maxgraph.v2.common.schema.TypeDef;

public class CreateEdgeTypeExecutor extends AbstractCreateTypeExecutor {
    
    @Override
    protected Operation makeOperation(int partition, long version, TypeDef typeDef, GraphDef newGraphDef) {
        return new CreateEdgeTypeOperation(partition, version, typeDef);
    }
}
