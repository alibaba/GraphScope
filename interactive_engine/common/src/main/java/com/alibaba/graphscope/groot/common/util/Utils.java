package com.alibaba.graphscope.groot.common.util;

import com.alibaba.graphscope.groot.common.constant.LogConstant;
import com.google.gson.JsonObject;

public class Utils {

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
    public static JsonObject buildMetricJsonLog(
            boolean isSuccess,
            String traceId,
            Integer batchSize,
            Integer partitionId,
            long cost,
            Long endTime,
            String stage,
            String logType) {
        JsonObject metricJsonLog = new JsonObject();
        metricJsonLog.addProperty(LogConstant.TRACE_ID, traceId);
        metricJsonLog.addProperty(LogConstant.SUCCESS, isSuccess);
        if (batchSize != null) {
            metricJsonLog.addProperty(LogConstant.BATCH_SIZE, batchSize);
        }
        if (partitionId != null) {
            metricJsonLog.addProperty(LogConstant.PARTITION_ID, partitionId);
        }
        if (endTime != null) {
            metricJsonLog.addProperty(LogConstant.END_TIME, endTime);
        }
        metricJsonLog.addProperty(LogConstant.COST, cost);
        if (stage != null) {
            metricJsonLog.addProperty(LogConstant.STAGE, stage);
        }
        metricJsonLog.addProperty(LogConstant.LOG_TYPE, logType);
        return metricJsonLog;
    }
}
