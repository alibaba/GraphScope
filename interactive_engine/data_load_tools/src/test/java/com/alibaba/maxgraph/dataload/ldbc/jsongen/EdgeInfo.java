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
package com.alibaba.maxgraph.dataload.ldbc.jsongen;

import com.alibaba.maxgraph.dataload.databuild.FileColumnMapping;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class EdgeInfo {
    abstract String getLabel();

    abstract String getSrcLabel();

    abstract String getDstLabel();

    abstract String[] getPropertyNames();

    public FileColumnMapping toFileColumnMapping() {
        String label = getLabel();
        String srcLabel = getSrcLabel();
        String dstLabel = getDstLabel();
        String[] propertyNames = getPropertyNames();
        Map<Integer, String> colMapping = new HashMap<>();
        for (int i = 0; i < propertyNames.length; i++) {
            colMapping.put(i + 2, propertyNames[i]);
        }
        Map<Integer, String> srcColMapping = Collections.singletonMap(0, "id");
        Map<Integer, String> dstColMapping = Collections.singletonMap(1, "id");
        return new FileColumnMapping(
                label, srcLabel, dstLabel, srcColMapping, dstColMapping, colMapping);
    }
}
