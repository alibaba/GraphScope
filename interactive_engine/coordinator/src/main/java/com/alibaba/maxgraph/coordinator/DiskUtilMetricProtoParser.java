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

import com.alibaba.maxgraph.proto.DiskMetricProto;
import com.alibaba.maxgraph.proto.MetricInfoProto;
import com.alibaba.maxgraph.proto.ServerMetricValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DiskUtilMetricProtoParser implements MetricProtoParser {
    @Override
    public List<ServerMetricValue> parser(Map<Integer, MetricInfoProtoBasedTime> metricInfoMap, String metricName) {
        List<ServerMetricValue> result = new ArrayList<>();
        metricInfoMap.forEach((serverId, metricBasedTime) -> {
            long timestamp = metricBasedTime.timestamp;
            MetricInfoProto infoProto = metricBasedTime.infoProto;
            List<DiskMetricProto> metrics = infoProto.getDiskMetricsList();
            if (metrics != null && !metrics.isEmpty()) {
                ServerMetricValue.Builder build = ServerMetricValue.newBuilder();
                build.setTimestamp(timestamp).setServerId(serverId).addAllDiskValue(metrics);
                result.add(build.build());
            }
        });
        return result;
    }
}
