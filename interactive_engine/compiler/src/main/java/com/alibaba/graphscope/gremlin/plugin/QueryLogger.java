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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryLogger {
    private static final Logger defaultLogger = LoggerFactory.getLogger(QueryLogger.class);
    private static Logger metricLogger = LoggerFactory.getLogger("MetricLog");

    private final String query;
    private final long queryId;

    public QueryLogger(String query, long queryId) {
        this.query = query;
        this.queryId = queryId;
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

    public void metricsInfo(String format, Object... args) {
        metricLogger.info(queryId + " | " + query + " | " + format, args);
    }

    @Override
    public String toString() {
        return "[" + "query='" + query + '\'' + ", queryId=" + queryId + ']';
    }

    public String getQuery() {
        return query;
    }

    public long getQueryId() {
        return queryId;
    }
}
