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

package com.alibaba.graphscope.gremlin.result;

import com.alibaba.graphscope.common.jna.type.FfiKeyType;
import com.alibaba.graphscope.gaia.proto.Common;
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.alibaba.graphscope.gremlin.exception.GremlinResultParserException;
import com.alibaba.graphscope.gremlin.transform.alias.AliasManager;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.*;

// values("name") -> key: head, value: "marko"
// valueMap("name") -> key: head, value: {name, "marko"}
// select("a").by("name") -> key: head, value: "marko"
// select("a", "b").by("name") -> key: a, value: "marko"; key: b, value: "josh"
// select("a", "b").by(valueMap("name")) -> key: a, value: {name, "marko"}; key: b, value:
// {name, "josh"}
public class ProjectResultParser extends LabelParser implements GremlinResultParser {
    private final Step step;

    private ProjectResultParser(Step step) {
        this.step = step;
    }

    public static ProjectResultParser create(Step step) {
        return new ProjectResultParser(step);
    }

    @Override
    public Object parseFrom(IrResult.Results results) {
        IrResult.Record record = results.getRecord();
        Map<Object, Object> projectResult = new LinkedHashMap<>();
        record.getColumnsList()
                .forEach(
                        column -> {
                            String tag = getColumnKeyAsResultKey(column.getNameOrId());
                            Object parseEntry = ParserUtils.parseEntry(column.getEntry());
                            if (parseEntry instanceof Map) {
                                Map projectTags = (Map) parseEntry;
                                // return empty Map if none properties
                                Map tagEntry =
                                        (Map)
                                                projectResult.computeIfAbsent(
                                                        tag, k1 -> new LinkedHashMap<>());
                                tagEntry.putAll(flatMap(projectTags));
                            } else {
                                if (!(parseEntry instanceof EmptyValue)) {
                                    projectResult.put(tag, parseEntry);
                                }
                            }
                        });
        Map<String, Object> parseLabel = (Map) parseLabelInProjectResults(projectResult, step);
        if (parseLabel.isEmpty()) {
            return EmptyValue.INSTANCE;
        } else if (parseLabel.size() == 1) {
            return parseLabel.entrySet().iterator().next().getValue();
        } else {
            return parseLabel;
        }
    }

    // {~label: "person", ~all: {name: "marko"}} -> {~label: "person", name: "marko"}
    private Map<Object, Object> flatMap(Map<String, Object> map) {
        Map<Object, Object> result = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            Object k = entry.getKey();
            Object v = entry.getValue();
            if (v instanceof Map) {
                result.putAll(flatMap((Map<String, Object>) v));
            } else {
                if (!(v instanceof EmptyValue)) {
                    String nameOrId = null;
                    if (k instanceof List) { // valueMap("name") -> Map<["", "name"], value>
                        nameOrId = (String) ((List) k).get(1);
                    } else if (k instanceof String) { // valueMap() -> Map<"name",
                        // value>
                        nameOrId = (String) k;
                    } else if (k instanceof Number) { // valueMap() -> Map<1, value>
                        nameOrId = String.valueOf(k);
                    }
                    if (nameOrId == null || nameOrId.isEmpty()) {
                        throw new GremlinResultParserException(
                                "map value should have property" + " key");
                    }
                    String property = getPropertyName(nameOrId);
                    if (step instanceof PropertyMapStep) {
                        result.put(property, Collections.singletonList(v));
                    } else if (property.equals(T.id.getAccessor())) {
                        result.put(T.id, v);
                    } else if (property.equals(T.label.getAccessor())) {
                        result.put(T.label, v);
                    } else {
                        result.put(property, v);
                    }
                }
            }
        }
        return result;
    }

    // a_1 -> a, i.e. g.V().as("a").select("a")
    // name_1 -> name, i.e. g.V().values("name")
    // a_name_1 -> a, i.e. g.V().as("a").select("a").by("name")
    private String getColumnKeyAsResultKey(Common.NameOrId columnKey) {
        if (columnKey.getItemCase() == Common.NameOrId.ItemCase.ITEM_NOT_SET) {
            return "";
        }
        switch (columnKey.getItemCase()) {
            case ITEM_NOT_SET:
                return "";
            case NAME:
                String key = columnKey.getName();
                return AliasManager.getPrefix(key);
            case ID:
                return String.valueOf(columnKey.getId());
            default:
                throw new GremlinResultParserException(columnKey.getItemCase() + " is invalid");
        }
    }

    // propertyId is in String format, i.e. "1"
    private String getPropertyName(String nameOrId) {
        Common.NameOrId.Builder builder = Common.NameOrId.newBuilder();
        if (nameOrId.matches("^[0-9]+$")) {
            builder.setId(Integer.valueOf(nameOrId));
        } else {
            builder.setName(nameOrId);
        }
        return ParserUtils.getKeyName(builder.build(), FfiKeyType.Column);
    }
}
