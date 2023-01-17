package com.alibaba.graphscope.groot.dataload;

import com.alibaba.graphscope.compiler.api.schema.GraphEdge;
import com.alibaba.graphscope.compiler.api.schema.GraphElement;
import com.alibaba.graphscope.groot.dataload.databuild.ColumnMappingInfo;
import com.alibaba.graphscope.groot.sdk.GrootClient;
import com.alibaba.graphscope.sdkcommon.common.DataLoadTarget;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CommitDataCommand extends DataCommand {

    public CommitDataCommand(String dataPath, boolean isFromOSS, String uniquePath)
            throws IOException {
        super(dataPath, isFromOSS, uniquePath);
    }

    public void run() {
        GrootClient client =
                GrootClient.newBuilder()
                        .setHosts(graphEndpoint)
                        .setUsername(username)
                        .setPassword(password)
                        .build();
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
        client.commitDataLoad(tableToTarget, this.uniquePath);
    }
}
