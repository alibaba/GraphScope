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
package com.alibaba.maxgraph.structure.manager.record;

import com.alibaba.maxgraph.sdkcommon.graph.ElementId;

public class DelEdgeManager extends AbstractEdgeManager implements RecordManager {
    private String label;
    private long edgeId;

    public DelEdgeManager(String label, long edgeId, ElementId srcId, ElementId dstId) {
        super(srcId, dstId);
        this.label = label;
        this.edgeId = edgeId;
    }

    public String getLabel() {
        return label;
    }

    public long getEdgeId() {
        return edgeId;
    }
}
