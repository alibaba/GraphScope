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
    // if query cost large than threshold, will print detail log
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
        onErrorEnd(null, msg);
    }

    public void onErrorEnd(@Nullable Throwable t) {
        this.metricsCollector.stop();
        onErrorEnd(t, null);
    }

    private void onErrorEnd(Throwable t, String msg) {
        String errorMsg = msg;
        if (t != null) {
            errorMsg = t.getMessage();
        }
        JsonObject logJson = buildSimpleLog(false, metricsCollector.getElapsedMillis());
        fillLogDetail(logJson, errorMsg, metricsCollector.getStartMillis(), null);
        queryLogger.print(logJson.toString(), false, t);

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
        JsonObject logJson = buildSimpleLog(true, metricsCollector.getElapsedMillis());
        if (this.metricsCollector.getElapsedMillis() > this.printThreshold) {
            fillLogDetail(logJson, null, metricsCollector.getStartMillis(), results);
        }
        queryLogger.print(logJson.toString(), true, null);

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

    private JsonObject buildSimpleLog(boolean isSucceed, long elaspedMillis) {
        String traceId = Span.current().getSpanContext().getTraceId();
        JsonObject simpleJson = new JsonObject();
        simpleJson.addProperty(LogConstant.TRACE_ID, traceId);
        simpleJson.addProperty(LogConstant.QUERY_ID, queryLogger.getQueryId());
        simpleJson.addProperty(LogConstant.SUCCESS, isSucceed);
        if (queryLogger.getUpStreamId() != null) {
            simpleJson.addProperty(LogConstant.UP_STREAM_ID, queryLogger.getUpStreamId());
        }
        simpleJson.addProperty(LogConstant.COST, elaspedMillis);
        return simpleJson;
    }

    private void fillLogDetail(
            JsonObject logJson, String errorMessage, long startMillis, List<Object> results) {
        logJson.addProperty(LogConstant.QUERY, queryLogger.getQuery());
        if (results != null) {
            logJson.addProperty(LogConstant.RESULT, JSON.toJson(results));
        }
        if (errorMessage != null) {
            logJson.addProperty(LogConstant.ERROR_MESSAGE, errorMessage);
        }
        logJson.addProperty(LogConstant.IR_PLAN, queryLogger.getIrPlan());
        logJson.addProperty(LogConstant.STAGE, "java");
        logJson.addProperty(LogConstant.START_TIME, startMillis);
    }

    public QueryLogger getQueryLogger() {
        return queryLogger;
    }
}
