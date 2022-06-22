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

package com.alibaba.graphscope.common.intermediate.operator;

import java.util.Collections;
import java.util.Map;

// contains info to represent subgraph in ir plan
public class SubGraphAsUnionOp extends UnionOp {
    // graph_name, i.e name: graph_XX
    private Map<String, String> graphConfigs;

    public SubGraphAsUnionOp(Map<String, String> graphConfigs) {
        super();
        this.graphConfigs = graphConfigs;
    }

    public Map<String, String> getGraphConfigs() {
        return graphConfigs == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(graphConfigs);
    }
}
