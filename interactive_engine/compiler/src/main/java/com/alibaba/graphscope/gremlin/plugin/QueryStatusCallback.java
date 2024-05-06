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

import com.codahale.metrics.Histogram;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.common.Attributes;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.Map;

import static io.opentelemetry.api.common.AttributeKey.*;

public class QueryStatusCallback {
    private final MetricsCollector metricsCollector;
    private final QueryLogger queryLogger;

    private LongHistogram histogram;

    public QueryStatusCallback(MetricsCollector metricsCollector, LongHistogram histogram, QueryLogger queryLogger) {
        this.metricsCollector = metricsCollector;
        this.queryLogger = queryLogger;
        this.histogram = histogram;
    }

    public void onStart() {}

    public void onEnd(boolean isSucceed, @Nullable String msg) {
        this.metricsCollector.stop();
        if (isSucceed) {
            queryLogger.info("total execution time is {} ms", metricsCollector.getElapsedMillis());
        }

        Attributes attrs =
                Attributes.of(
                        stringKey("id"), queryLogger.getQueryId().toString(),
                        stringKey("query"), queryLogger.getQuery(),
                        booleanKey("isSucceed"), isSucceed);
        this.histogram.record(metricsCollector.getElapsedMillis(), attrs);
        
        queryLogger.metricsInfo(
                "{} | {} | {} | {}",
                isSucceed,
                metricsCollector.getElapsedMillis(),
                metricsCollector.getStartMillis(),
                msg != null ? msg : StringUtils.EMPTY);
    }

    public QueryLogger getQueryLogger() {
        return queryLogger;
    }
}
