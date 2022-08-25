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

import com.alibaba.graphscope.gaia.proto.IrResult;
import com.alibaba.graphscope.gaia.proto.OuterExpression;
import com.alibaba.graphscope.gremlin.Utils;
import com.alibaba.graphscope.gremlin.exception.GremlinResultParserException;
import com.alibaba.graphscope.gremlin.plugin.step.GroupCountStep;
import com.alibaba.graphscope.gremlin.plugin.step.GroupStep;
import com.alibaba.graphscope.gremlin.transform.alias.AliasManager;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class GroupResultParser implements GremlinResultParser {
    private static final Logger logger = LoggerFactory.getLogger(GroupResultParser.class);
    private Map<String, KeyValueType> queryGivenAliasTypeMap;

    public static GroupResultParser create(Step groupStep) {
        return new GroupResultParser(groupStep);
    }

    private GroupResultParser(Step groupStep) {
        this.queryGivenAliasTypeMap = new HashMap<>();
        List<Traversal.Admin> keyTraversals, valueTraversals;
        if (Utils.equalClass(groupStep, GroupStep.class)) {
            keyTraversals = ((GroupStep) groupStep).getKeyTraversalList();
            valueTraversals = ((GroupStep) groupStep).getValueTraversalList();
        } else if (Utils.equalClass(groupStep, GroupCountStep.class)) {
            keyTraversals = ((GroupCountStep) groupStep).getKeyTraversalList();
            valueTraversals = ((GroupCountStep) groupStep).getValueTraversalList();
        } else {
            throw new GremlinResultParserException("invalid type " + groupStep.getClass());
        }
        keyTraversals.forEach(
                k -> {
                    Step endStep = k.getEndStep();
                    if (!endStep.getLabels().isEmpty()) {
                        if (endStep.getLabels().size() > 1) {
                            logger.error(
                                    "multiple aliases of one object is unsupported, take the first"
                                            + " and ignore others");
                        }
                        String label = (String) endStep.getLabels().iterator().next();
                        queryGivenAliasTypeMap.put(label, KeyValueType.Key);
                    }
                });
        valueTraversals.forEach(
                v -> {
                    Step endStep = v.getEndStep();
                    if (!endStep.getLabels().isEmpty()) {
                        if (endStep.getLabels().size() > 1) {
                            logger.error(
                                    "multiple aliases of one object is unsupported, take the first"
                                            + " and ignore others");
                        }
                        String label = (String) endStep.getLabels().iterator().next();
                        queryGivenAliasTypeMap.put(label, KeyValueType.Value);
                    }
                });
    }

    @Override
    public Object parseFrom(IrResult.Results results) {
        logger.debug("{}", results);
        IrResult.Record record = results.getRecord();
        List<Object> keys = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        for (IrResult.Column column : record.getColumnsList()) {
            OuterExpression.NameOrId columnName = column.getNameOrId();
            if (columnName.getItemCase() != OuterExpression.NameOrId.ItemCase.NAME) {
                throw new GremlinResultParserException(
                        "column key in group should be ItemCase.NAME");
            }
            Object parseEntry = ParserUtils.parseEntry(column.getEntry());
            if (parseEntry instanceof EmptyValue) parseEntry = null;
            String alias = columnName.getName();
            if (AliasManager.isGroupKeysPrefix(alias) || isKeyType(alias)) {
                keys.add(parseEntry);
            } else {
                values.add(parseEntry);
            }
        }
        Object groupKey = (keys.size() == 1) ? keys.get(0) : keys;
        Object groupValue = (values.size() == 1) ? values.get(0) : values;
        // if value is null then ignore, i.e.
        // g.V().values("age").group() => {35=[35], 27=[27], 32=[32], 29=[29]}
        if (groupValue == null) return EmptyValue.INSTANCE;
        // if key is null then output null key with the corresponding value, i.e.
        // g.V().group().by("age") => {29=[v[1]], null=[v[72057594037927939],
        // v[72057594037927941]], 27=[v[2]], 32=[v[4]], 35=[v[6]]}
        return Collections.singletonMap(groupKey, groupValue);
    }

    private boolean isKeyType(String queryGivenAlias) {
        KeyValueType type = queryGivenAliasTypeMap.get(queryGivenAlias);
        return type != null && type == KeyValueType.Key;
    }

    private enum KeyValueType {
        Key,
        Value
    }
}
