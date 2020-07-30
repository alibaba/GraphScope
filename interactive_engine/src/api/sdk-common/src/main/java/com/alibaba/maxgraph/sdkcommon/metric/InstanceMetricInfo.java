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
package com.alibaba.maxgraph.sdkcommon.metric;

import com.alibaba.maxgraph.proto.AllMetricsInfoProto;
import com.alibaba.maxgraph.proto.DiskMetricProto;
import com.alibaba.maxgraph.proto.ServerMetricValue;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public class InstanceMetricInfo {

    // serverId -> ServerMetricInfo
    public final Map<Integer, ServerMetricInfo> serverMetricInfoMap = Maps.newHashMap();

    public InstanceMetricInfo (List<AllMetricsInfoProto> allMetricsInfoProtoList) {
        for (AllMetricsInfoProto metricsInfoProto : allMetricsInfoProtoList) {
            for (ServerMetricValue serverMetricValue : metricsInfoProto.getValuesList()) {
                if (!serverMetricInfoMap.containsKey(serverMetricValue.getServerId())) {
                    serverMetricInfoMap.put(serverMetricValue.getServerId(), new ServerMetricInfo());
                }

                ServerMetricInfo serverMetricInfo = serverMetricInfoMap.get(serverMetricValue.getServerId());
                serverMetricInfo.addMetricValue(metricsInfoProto.getMetricName(), serverMetricValue);
            }
        }
    }

    public static class ServerMetricInfo {
        public final Map<String, MetricValue> metricValueMap = Maps.newHashMap();

        public void addMetricValue(String metricName, ServerMetricValue serverMetricValue) {
            metricValueMap.put(metricName, new MetricValue(serverMetricValue));
        }
    }

    public static class MetricValue {
        public final int serverId;
        public final long timeStamp;
        public final String value;
        public List<DiskMetricValue> diskMetricValues;

        public MetricValue (ServerMetricValue serverMetricValue) {
            this.serverId = serverMetricValue.getServerId();
            this.timeStamp = serverMetricValue.getTimestamp();
            this.value = serverMetricValue.getValue();

            if (serverMetricValue.getDiskValueCount() != 0) {
                diskMetricValues = Lists.newArrayList();
                for (DiskMetricProto diskMetricProto : serverMetricValue.getDiskValueList()) {
                    diskMetricValues.add(new DiskMetricValue(diskMetricProto));
                }
            }
        }
    }

    public static class DiskMetricValue {
        public final String path;
        public final long usage;
        public final long available ;
        public final long total;

        public DiskMetricValue(DiskMetricProto diskMetricProto) {
            this.path = diskMetricProto.getPath();
            this.usage = diskMetricProto.getUsage();
            this.available = diskMetricProto.getAvailable();
            this.total = diskMetricProto.getTotal();
        }

        public DiskMetricValue(String path, long usage, long available, long total) {
            this.path = path;
            this.usage = usage;
            this.available = available;
            this.total = total;
        }

        public String getPath() {
            return path;
        }

        public long getUsage() {
            return usage;
        }

        public long getAvailable() {
            return available;
        }

        public long getTotal() {
            return total;
        }
    }
}
