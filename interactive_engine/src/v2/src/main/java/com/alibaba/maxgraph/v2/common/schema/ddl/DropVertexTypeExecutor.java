package com.alibaba.maxgraph.v2.common.schema.ddl;

import com.alibaba.maxgraph.v2.common.operation.LabelId;
import com.alibaba.maxgraph.v2.common.operation.Operation;
import com.alibaba.maxgraph.v2.common.operation.ddl.DropVertexTypeOperation;

public class DropVertexTypeExecutor extends AbstractDropTypeExecutor {

    @Override
    protected Operation makeOperation(int partition, long version, LabelId labelId) {
        return new DropVertexTypeOperation(partition, version, labelId);
    }
}
