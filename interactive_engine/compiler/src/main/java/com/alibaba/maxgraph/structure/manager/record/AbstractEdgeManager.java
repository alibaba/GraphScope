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

public abstract class AbstractEdgeManager {
    private ElementId srcId;
    private ElementId dstId;

    public AbstractEdgeManager(ElementId srcId, ElementId dstId) {
        this.srcId = srcId;
        this.dstId = dstId;
    }

    public ElementId getSrcId() {
        return this.srcId;
    }

    public ElementId getDstId() {
        return this.dstId;
    }
}
