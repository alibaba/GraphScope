/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.gremlin.plugin;

import com.alibaba.graphscope.groot.common.constant.LogConstant;
import com.alibaba.graphscope.groot.common.util.JSON;
import com.google.gson.JsonObject;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.trace.Span;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class QueryStatusCallback {
    private final MetricsCollector metricsCollector;
    private final QueryLogger queryLogger;

    private LongHistogram queryHistogram;
    private long printThreshold;

    public QueryStatusCallback(
            MetricsCollector metricsCollector,
            LongHistogram histogram,
            QueryLogger queryLogger,
            long printThreshold) {
        this.metricsCollector = metricsCollector;
        this.queryLogger = queryLogger;
        this.queryHistogram = histogram;
        this.printThreshold = printThreshold;
    }

    public void onStart() {}

    public void onErrorEnd(@Nullable String msg) {
        this.metricsCollector.stop();
        JsonObject detailLog =
                buildDetailLog(
                        false,
                        msg,
                        metricsCollector.getElapsedMillis(),
                        metricsCollector.getStartMillis(),
                        null);
        queryLogger.print(detailLog.toString(), false, null);

        Attributes attrs =
                Attributes.builder()
                        .put("id", queryLogger.getQueryId().toString())
                        .put("query", queryLogger.getQuery())
                        .put("success", false)
                        .put("message", msg != null ? msg : "")
                        .build();
        this.queryHistogram.record(metricsCollector.getElapsedMillis(), attrs);
        queryLogger.metricsInfo(false, metricsCollector.getElapsedMillis());
    }

    public void onErrorEnd(@Nullable Throwable t) {
        this.metricsCollector.stop();
        String msg = t.getMessage();
        JsonObject detailLog =
                buildDetailLog(
                        false,
                        msg,
                        metricsCollector.getElapsedMillis(),
                        metricsCollector.getStartMillis(),
                        null);
        queryLogger.print(detailLog.toString(), false, t);

        Attributes attrs =
                Attributes.builder()
                        .put("id", queryLogger.getQueryId().toString())
                        .put("query", queryLogger.getQuery())
                        .put("success", false)
                        .put("message", msg != null ? msg : "")
                        .build();
        this.queryHistogram.record(metricsCollector.getElapsedMillis(), attrs);
        queryLogger.metricsInfo(false, metricsCollector.getElapsedMillis());
    }

    public void onSuccessEnd(List<Object> results) {
        this.metricsCollector.stop();
        if (this.metricsCollector.getElapsedMillis() > this.printThreshold) {
            JsonObject detailLog =
                    buildDetailLog(
                            true,
                            null,
                            metricsCollector.getElapsedMillis(),
                            metricsCollector.getStartMillis(),
                            results);
            queryLogger.print(detailLog.toString(), true, null);
        }

        Attributes attrs =
                Attributes.builder()
                        .put("id", queryLogger.getQueryId().toString())
                        .put("query", queryLogger.getQuery())
                        .put("success", true)
                        .put("message", "")
                        .build();
        this.queryHistogram.record(metricsCollector.getElapsedMillis(), attrs);
        queryLogger.metricsInfo(true, metricsCollector.getElapsedMillis());
    }

    private JsonObject buildDetailLog(
            boolean isSucceed,
            String errorMessage,
            long elaspedMillis,
            long startMillis,
            List<Object> results) {
        String traceId = Span.current().getSpanContext().getTraceId();
        JsonObject detailJson = new JsonObject();
        detailJson.addProperty(LogConstant.TRACE_ID, traceId);
        detailJson.addProperty(LogConstant.QUERY_ID, queryLogger.getQueryId());
        detailJson.addProperty(LogConstant.QUERY, queryLogger.getQuery());
        detailJson.addProperty(LogConstant.SUCCESS, isSucceed);
        if (queryLogger.getUpTraceId() != null) {
            detailJson.addProperty(LogConstant.UP_TRACE_ID, queryLogger.getUpTraceId());
        }
        if (errorMessage != null) {
            detailJson.addProperty(LogConstant.ERROR_MESSAGE, errorMessage);
        }
        if (results != null) {
            detailJson.addProperty(LogConstant.RESULT, JSON.toJson(results));
        }
        detailJson.addProperty(LogConstant.IR_PLAN, queryLogger.getIrPlan());
        detailJson.addProperty(LogConstant.STAGE, "java");
        detailJson.addProperty(LogConstant.COST, elaspedMillis);
        detailJson.addProperty(LogConstant.START_TIME, startMillis);
        return detailJson;
    }

    public QueryLogger getQueryLogger() {
        return queryLogger;
    }
}
