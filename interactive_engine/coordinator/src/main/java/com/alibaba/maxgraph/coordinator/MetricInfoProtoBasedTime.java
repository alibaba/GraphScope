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

public class MetricInfoProtoBasedTime {
    public long timestamp;
    public MetricInfoProto infoProto;
    private int nodeId;

    public MetricInfoProtoBasedTime(long timestamp, MetricInfoProto infoProto, int nodeId) {
        this.timestamp = timestamp;
        this.infoProto = infoProto;
        this.nodeId = nodeId;
    }

    public int getNodeId() {
        return nodeId;
    }

    @Override
    public String toString() {
        return "MetricInfoProtoBasedTime{" +
                "timestamp=" + timestamp +
                ", infoProto=" + infoProto +
                '}';
    }
}
