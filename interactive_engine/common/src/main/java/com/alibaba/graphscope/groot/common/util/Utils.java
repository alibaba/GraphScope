package com.alibaba.graphscope.groot.common.util;

import com.alibaba.graphscope.groot.common.constant.LogConstant;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

    private static final ObjectMapper jsonMapper = new ObjectMapper();
    private static final Logger defaultLogger = LoggerFactory.getLogger(Utils.class);

    /**
     * build metric json log for monitor
     * plz dont delete any field
     * could add new field
     * @param isSuccess
     * @param traceId
     * @param batchSize
     * @param partitionId
     * @param cost
     * @param endTime
     * @param stage
     * @param logType
     * @return
     */
    public static String buildMetricJsonLog(
            boolean isSuccess,
            String traceId,
            Integer batchSize,
            Integer partitionId,
            long cost,
            Long endTime,
            String stage,
            String logType) {
        String jsonLog = "";
        ObjectNode metricJsonLog = jsonMapper.createObjectNode();
        metricJsonLog.put(LogConstant.TRACE_ID, traceId);
        metricJsonLog.put(LogConstant.SUCCESS, isSuccess);
        if (batchSize != null) {
            metricJsonLog.put(LogConstant.BATCH_SIZE, batchSize);
        }
        if (partitionId != null) {
            metricJsonLog.put(LogConstant.PARTITION_ID, partitionId);
        }
        if (endTime != null) {
            metricJsonLog.put(LogConstant.END_TIME, endTime);
        }
        metricJsonLog.put(LogConstant.COST, cost);
        if (stage != null) {
            metricJsonLog.put(LogConstant.STAGE, stage);
        }
        metricJsonLog.put(LogConstant.LOG_TYPE, logType);
        try {
            jsonLog = jsonMapper.writeValueAsString(metricJsonLog);
        } catch (Exception e) {
            defaultLogger.error("JsonProcessingException!", e);
        }
        return jsonLog;
    }
}
