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
package com.alibaba.maxgraph.dataload.databuild;

import java.util.Map;

public class ColumnMappingInfo {

    private int labelId;
    private int srcLabelId;
    private int dstLabelId;
    private long tableId;

    private Map<Integer, Integer> srcPkColMap;
    private Map<Integer, Integer> dstPkColMap;
    private Map<Integer, Integer> propertiesColMap;

    public ColumnMappingInfo() {}

    public ColumnMappingInfo(int labelId, long tableId, Map<Integer, Integer> propertiesColMap) {
        this(labelId, -1, -1, tableId, null, null, propertiesColMap);
    }

    public ColumnMappingInfo(int labelId, int srcLabelId, int dstLabelId, long tableId,
                             Map<Integer, Integer> srcPkColMap, Map<Integer, Integer> dstPkColMap,
                             Map<Integer, Integer> propertiesColMap) {
        this.labelId = labelId;
        this.srcLabelId = srcLabelId;
        this.dstLabelId = dstLabelId;
        this.tableId = tableId;
        this.srcPkColMap = srcPkColMap;
        this.dstPkColMap = dstPkColMap;
        this.propertiesColMap = propertiesColMap;
    }

    public long getTableId() {
        return tableId;
    }

    public int getLabelId() {
        return labelId;
    }

    public int getSrcLabelId() {
        return srcLabelId;
    }

    public int getDstLabelId() {
        return dstLabelId;
    }

    public Map<Integer, Integer> getSrcPkColMap() {
        return srcPkColMap;
    }

    public Map<Integer, Integer> getDstPkColMap() {
        return dstPkColMap;
    }

    public Map<Integer, Integer> getPropertiesColMap() {
        return propertiesColMap;
    }
}
