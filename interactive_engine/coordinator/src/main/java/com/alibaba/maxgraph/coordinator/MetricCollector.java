/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.coordinator;

import com.alibaba.maxgraph.proto.MetricInfoProto;
import com.alibaba.maxgraph.proto.ServerMetricValue;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class MetricCollector {
    private static final Logger LOG = LoggerFactory.getLogger(MetricCollector.class);

    // private Map<String, Map<Integer, MetricInfoProto>> metricMap;
    private Map<Integer, MetricInfoProtoBasedTime> metricInfoProtoMap;
    private Map<String, MetricProtoParser> protoParserMap;

    public MetricCollector() {
        metricInfoProtoMap = Maps.newConcurrentMap();
        protoParserMap = Maps.newHashMap();
    }

    public void registerMetricProtoParser(String metricName, MetricProtoParser parser) {
        protoParserMap.put(metricName, parser);
    }

    public void updateMetrics(int serverId, MetricInfoProto infoProto, int nodeId) {
        LOG.debug("serverId is {} infoProto is {}", serverId, infoProto);
        long currentTime = System.currentTimeMillis();
        metricInfoProtoMap.put(
                serverId, new MetricInfoProtoBasedTime(currentTime, infoProto, nodeId));
        LOG.debug("metricInfoProtoMap is {}", metricInfoProtoMap);
    }

    public List<ServerMetricValue> getMetricByName(String metricName) {
        try {
            MetricProtoParser protoParser = protoParserMap.get(metricName);
            return protoParser.parser(metricInfoProtoMap, metricName);
        } catch (Exception e) {
            LOG.error("getMetricByName {} fail {}", metricName, e);
            return new ArrayList<>();
        }
    }

    public Map<String, List<ServerMetricValue>> getAllMetrics() {
        Map<String, List<ServerMetricValue>> result = Maps.newHashMap();
        for (Map.Entry<String, MetricProtoParser> parserEntry : protoParserMap.entrySet()) {
            List<ServerMetricValue> valueList =
                    parserEntry.getValue().parser(metricInfoProtoMap, parserEntry.getKey());
            if (valueList != null && !valueList.isEmpty()) {
                result.put(parserEntry.getKey(), valueList);
            }
        }
        LOG.debug("getAllMetrics result is {}", result);
        return result;
    }
}
