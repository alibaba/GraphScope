/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.common;

import com.alibaba.maxgraph.proto.MetricInfoProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * different modules (frontend/executor) use the class to report and record monitor metrics
 * reportGauge is the main function to report the metric
 * the class is thread-safe
 */
public class MetricReporter {
    private static final Logger LOG = LoggerFactory.getLogger(MetricReporter.class);
    private List<MetricGetter> metricGetterList;

    public MetricReporter() {
        metricGetterList = new ArrayList<>();
    }

    public synchronized void registerMetric(MetricGetter getter) {
        metricGetterList.add(getter);
    }

    public synchronized MetricInfoProto reportLatestMetrics() {
        MetricInfoProto.Builder build = MetricInfoProto.newBuilder();
        for (MetricGetter getter : metricGetterList) {
            try {
                Map<String, String> cur = getter.getMetric();
                if (cur != null) {
                    build.putAllMetricInfo(cur);
                }
            } catch (Exception e) {
                LOG.warn("collect metrics failed", e);
            }
        }
        LOG.debug("report metrics is  {}", build.build());
        return build.build();
    }
}
