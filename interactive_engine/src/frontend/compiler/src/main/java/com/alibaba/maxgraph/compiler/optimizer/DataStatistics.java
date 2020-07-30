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
package com.alibaba.maxgraph.compiler.optimizer;

import java.util.List;

public class DataStatistics {
    private static final long DEFAULT_VERTEX_VALUE_SIZE = 12;
    private static final long DEFAULT_EDGE_VALUE_SIZE = 12;

    private static final double DEFAULT_OUT_NUM_FACTOR = 10;
    private static final double DEFAULT_IN_NUM_FACTOR = 10;
    private static final double DEFAULT_UNFOLD_NUM_FACTOR = 10;

    public long getLabelVertexCount(List<String> vertexLabelList) {
        return 0;
    }

    public long getLabelEdgeCount(List<String> collect) {
        return 0;
    }

    public double getOutNumFactor() {
        return DEFAULT_OUT_NUM_FACTOR;
    }

    public double getInNumFactor() {
        return DEFAULT_IN_NUM_FACTOR;
    }

    public double getFilterFactor() {
        return 0.5;
    }

    public double getDedupFactor() {
        return 0.8;
    }

    public double getUnfoldFactor() {
        return DEFAULT_UNFOLD_NUM_FACTOR;
    }

    public long getVertexValueSize() {
        return DEFAULT_VERTEX_VALUE_SIZE;
    }

    public long getLongValueSize() {
        return 8;
    }

    public long getEdgeValueSize() {
        return DEFAULT_EDGE_VALUE_SIZE;
    }
}
