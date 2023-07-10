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

public class QueryStatusCallback {
    private final MetricsCollector metricsCollector;
    private final QueryLogger queryLogger;

    public QueryStatusCallback(MetricsCollector metricsCollector, QueryLogger queryLogger) {
        this.metricsCollector = metricsCollector;
        this.queryLogger = queryLogger;
    }

    public void onStart() {}

    public void onEnd(boolean isSucceed) {
        this.metricsCollector.stop();
        queryLogger.info("total execution time is {} ms", metricsCollector.getElapsedMillis());
        queryLogger.metricsInfo(
                "{} | {} | {}",
                isSucceed,
                metricsCollector.getElapsedMillis(),
                metricsCollector.getStartMillis());
    }

    public QueryLogger getQueryLogger() {
        return queryLogger;
    }
}
