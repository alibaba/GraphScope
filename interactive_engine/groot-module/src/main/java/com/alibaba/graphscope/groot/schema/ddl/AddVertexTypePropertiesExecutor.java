package com.alibaba.graphscope.groot.schema.ddl;

import com.alibaba.graphscope.groot.common.schema.wrapper.GraphDef;
import com.alibaba.graphscope.groot.common.schema.wrapper.LabelId;
import com.alibaba.graphscope.groot.common.schema.wrapper.TypeDef;
import com.alibaba.graphscope.groot.operation.Operation;
import com.alibaba.graphscope.groot.operation.ddl.AddVertexTypePropertiesOperation;

import java.util.Map;

public class AddVertexTypePropertiesExecutor extends AbstractAddTypePropertiesExecutor {

    @Override
    protected Operation makeOperation(
            int partition, long version, TypeDef typeDef, GraphDef newGraphDef) {
        Map<LabelId, Long> vertexTableIdMap = newGraphDef.getVertexTableIds();
        Long tableId = vertexTableIdMap.get(typeDef.getTypeLabelId());
        return new AddVertexTypePropertiesOperation(partition, version, typeDef, tableId);
    }
}
