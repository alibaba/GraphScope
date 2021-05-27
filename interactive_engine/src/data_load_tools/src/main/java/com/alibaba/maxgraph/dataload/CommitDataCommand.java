package com.alibaba.maxgraph.dataload;

import com.alibaba.maxgraph.dataload.databuild.ColumnMappingInfo;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.EdgeType;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.SchemaElement;
import com.alibaba.maxgraph.v2.sdk.Client;
import com.alibaba.maxgraph.v2.sdk.DataLoadTarget;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CommitDataCommand extends DataCommand {

    public CommitDataCommand(String dataPath) throws IOException {
        super(dataPath);
    }

    public void run() {
        Client client = new Client(graphEndpoint, "");
        Map<Long, DataLoadTarget> tableToTarget = new HashMap<>();
        for (ColumnMappingInfo columnMappingInfo : columnMappingInfos.values()) {
            long tableId = columnMappingInfo.getTableId();
            int labelId = columnMappingInfo.getLabelId();
            SchemaElement schemaElement = schema.getSchemaElement(labelId);
            String label = schemaElement.getLabel();
            DataLoadTarget.Builder builder = DataLoadTarget.newBuilder();
            builder.setLabel(label);
            if (schemaElement instanceof EdgeType) {
                builder.setSrcLabel(schema.getSchemaElement(columnMappingInfo.getSrcLabelId()).getLabel());
                builder.setDstLabel(schema.getSchemaElement(columnMappingInfo.getDstLabelId()).getLabel());
            }
            tableToTarget.put(tableId, builder.build());
        }
        client.commitDataLoad(tableToTarget);
    }
}
