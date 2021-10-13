package com.alibaba.maxgraph.dataload;

import com.alibaba.graphscope.groot.sdk.Client;
import com.alibaba.maxgraph.compiler.api.schema.GraphEdge;
import com.alibaba.maxgraph.compiler.api.schema.GraphElement;
import com.alibaba.maxgraph.dataload.databuild.ColumnMappingInfo;

import com.alibaba.maxgraph.sdkcommon.common.DataLoadTarget;
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
            GraphElement graphElement = schema.getElement(labelId);
            String label = graphElement.getLabel();
            DataLoadTarget.Builder builder = DataLoadTarget.newBuilder();
            builder.setLabel(label);
            if (graphElement instanceof GraphEdge) {
                builder.setSrcLabel(
                        schema.getElement(columnMappingInfo.getSrcLabelId()).getLabel());
                builder.setDstLabel(
                        schema.getElement(columnMappingInfo.getDstLabelId()).getLabel());
            }
            tableToTarget.put(tableId, builder.build());
        }
        client.commitDataLoad(tableToTarget);
    }
}
