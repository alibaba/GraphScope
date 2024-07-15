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
import com.google.gson.JsonObject;
import io.opentelemetry.api.trace.Span;
import jline.internal.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;

public class QueryLogger {
    private static final Logger defaultLogger = LoggerFactory.getLogger(QueryLogger.class);
    private static Logger metricLogger = LoggerFactory.getLogger("MetricLog");

    private final String query;
    private final BigInteger queryId;

    /**
     * 上游带下来的traceId
     */
    private final String upTraceId;
    private String irPlan;


    public QueryLogger(String query, BigInteger queryId) {
        this.query = query;
        this.queryId = queryId;
        this.irPlan = null;
        this.upTraceId = null;
    }

    public QueryLogger(String query, BigInteger queryId, String upTraceId) {
        this.query = query;
        this.queryId = queryId;
        this.upTraceId = upTraceId;
        this.irPlan = null;
    }

    public void debug(String format, Object... args) {
        defaultLogger.debug(this + " : " + format, args);
    }

    public void info(String format, Object... args) {
        defaultLogger.info(this + " : " + format, args);
    }

    public void warn(String format, Object... args) {
        defaultLogger.warn(this + " : " + format, args);
    }

    public void error(String format, Object... args) {
        defaultLogger.error(this + " : " + format, args);
    }


    public void error(Throwable throwable) {
        JsonObject errorJson = new JsonObject();
        String traceId = Span.current().getSpanContext().getTraceId();
        if (this.upTraceId != null) {
            errorJson.addProperty(LogConstant.UP_TRACE_ID, this.upTraceId);
        }
        errorJson.addProperty(LogConstant.TRACE_ID, traceId);
        errorJson.addProperty(LogConstant.SUCCESS, false);
        errorJson.addProperty(LogConstant.STAGE, "java");
        errorJson.addProperty(LogConstant.LOG_TYPE, "query");
        errorJson.addProperty(LogConstant.ERROR_MESSAGE, throwable.getMessage());
        defaultLogger.error(errorJson.toString(), throwable);
    }


    public void print(String message, boolean success, Throwable t) {
        if (success) {
            defaultLogger.info(message);
        } else {
            defaultLogger.error(message, t);
        }
    }

    public void metricsInfo(String format, Object... args) {
        metricLogger.info(queryId + " | " + query + " | " + format, args);
    }

    public void metricsInfo(boolean isSucceed, long cost) {
        JsonObject metricJson = new JsonObject();
        String traceId = Span.current().getSpanContext().getTraceId();
        if (this.upTraceId != null) {
            metricJson.addProperty(LogConstant.UP_TRACE_ID, this.upTraceId);
        }
        metricJson.addProperty(LogConstant.TRACE_ID, traceId);
        metricJson.addProperty(LogConstant.SUCCESS, isSucceed);
        metricJson.addProperty(LogConstant.COST, cost);
        metricJson.addProperty(LogConstant.STAGE, "java");
        metricJson.addProperty(LogConstant.LOG_TYPE, "query");
        metricLogger.info(metricJson.toString());
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append("[");
        if (this.upTraceId != null) {
            str.append("upTraceId=")
                    .append(this.upTraceId)
                    .append(", ");
        }
        str.append("query='")
                .append(this.query)
                .append("'")
                .append(", queryId=")
                .append(this.queryId)
                .append("]");
        return str.toString();
    }

    public String getQuery() {
        return query;
    }

    public BigInteger getQueryId() {
        return queryId;
    }

    public void setIrPlan(String irPlan) {
        this.irPlan = irPlan;
    }

    public String getUpTraceId() {
        return upTraceId;
    }

    public String getIrPlan() {
        return irPlan;
    }
}
