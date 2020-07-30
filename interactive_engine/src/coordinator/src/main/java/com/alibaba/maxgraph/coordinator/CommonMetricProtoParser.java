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
package com.alibaba.maxgraph.coordinator;

import com.alibaba.maxgraph.proto.MetricInfoProto;
import com.alibaba.maxgraph.proto.ServerMetricValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CommonMetricProtoParser implements MetricProtoParser {
    private static final Logger LOG = LoggerFactory.getLogger(CommonMetricProtoParser.class);

    @Override
    public List<ServerMetricValue> parser(Map<Integer, MetricInfoProtoBasedTime> metricInfoProtoMap, String metricName) {
        List<ServerMetricValue> result = new ArrayList<>();
        metricInfoProtoMap.forEach((serverId, metricBasedTime) -> {
            long timestamp = metricBasedTime.timestamp;
            MetricInfoProto infoProto = metricBasedTime.infoProto;
            String value = infoProto.getMetricInfoMap().get(metricName);
            if (value != null) {
                ServerMetricValue.Builder build = ServerMetricValue.newBuilder();
                result.add(build.setServerId(serverId).setTimestamp(timestamp).setValue(value).
                        setNodeId(metricBasedTime.getNodeId()).build());
            }
        });
        LOG.debug("getMetricByName result is {}", result);
        return result;
    }
}
