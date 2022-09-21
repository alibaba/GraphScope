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

package com.alibaba.graphscope.dataload;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;

@JsonIgnoreProperties({
    "vertex_type_hierarchy",
    "column_name_to_id",
    "vertex_columns",
    "edge_columns"
})
public class ColumnMappingMeta {
    // vertex is 0, edge is 1
    private Map<String, Integer> tableToElementType = new HashMap<>();
    private Map<String, String> tableToLabelName = new HashMap<>();
    private Map<String, Integer> labelNameToId = new HashMap<>();
    // table name -> src label and dst labels
    private Map<String, List<String>> tableToSDLabels = new HashMap<>();

    @JsonProperty("vertex_table_to_type")
    private Map<String, String> vertexTableToType;

    @JsonProperty("edge_table_to_type")
    private Map<String, Object> edgeTableToType;

    @JsonProperty("vertex_type_to_id")
    private Map<String, Integer> vertexTypeToId;

    @JsonProperty("edge_type_to_id")
    private Map<String, Integer> edgeTypeToId;

    public ColumnMappingMeta init() {
        vertexTableToType.forEach(
                (k, v) -> {
                    this.tableToElementType.put(k, 0);
                    this.tableToLabelName.put(k, v);
                });
        edgeTableToType.forEach(
                (k, v) -> {
                    this.tableToElementType.put(k, 1);
                    Map<String, String> labels = (Map<String, String>) v;
                    this.tableToLabelName.put(k, labels.get("etype"));
                    this.tableToSDLabels.put(
                            k,
                            Arrays.asList(
                                    labels.get("src_vertex_type"), labels.get("dst_vertex_type")));
                });
        this.labelNameToId.putAll(vertexTypeToId);
        this.labelNameToId.putAll(edgeTypeToId);
        return this;
    }

    public List<String> getTableNames() {
        return new ArrayList<>(this.tableToElementType.keySet());
    }

    public String getLabelName(String tableName) {
        return this.tableToLabelName.get(tableName);
    }

    public long getElementType(String tableName) {
        return this.tableToElementType.get(tableName);
    }

    public long getLabelId(String labelName) {
        return this.labelNameToId.get(labelName);
    }

    public String getSrcLabel(String tableName) {
        return this.tableToSDLabels.get(tableName).get(0);
    }

    public String getDstLabel(String tableName) {
        return this.tableToSDLabels.get(tableName).get(1);
    }

    @Override
    public String toString() {
        return "ColumnMappingMeta{"
                + "tableToElementType="
                + tableToElementType
                + ", tableToLabelName="
                + tableToLabelName
                + ", labelNameToId="
                + labelNameToId
                + ", vertexTableToType="
                + vertexTableToType
                + ", edgeTableToType="
                + edgeTableToType
                + ", vertexTypeToId="
                + vertexTypeToId
                + ", edgeTypeToId="
                + edgeTypeToId
                + '}';
    }
}
