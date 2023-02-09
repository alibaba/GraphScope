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

package com.alibaba.graphscope.common.calcite.tools;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

import com.alibaba.graphscope.common.calcite.rex.RexGraphVariable;
import com.alibaba.graphscope.common.calcite.util.Static;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.NlsString;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * infer a new alias name from the query given one, and validate if the query given alias is duplicated.
 */
public abstract class AliasInference {
    public static final String DEFAULT_NAME = "~DEFAULT";
    public static final int DEFAULT_ID = -1;

    public static final String SIMPLE_NAME(String alias) {
        return alias == DEFAULT_NAME ? "DEFAULT" : alias;
    }

    /**
     * infer alias for graph operators,
     * throw exceptions if fieldName has existed in {@code uniqueNameList}, otherwise return fieldName itself or {@link #DEFAULT_NAME}
     * @param fieldName
     * @param uniqueNameList alias names have stored by previous operators
     * @return
     * @throws IllegalArgumentException - if fieldName has existed in {@code uniqueNameList}
     */
    public static final String inferDefault(@Nullable String fieldName, Set<String> uniqueNameList)
            throws IllegalArgumentException {
        if (fieldName == null) return DEFAULT_NAME;
        if (uniqueNameList.contains(fieldName)) {
            throw new IllegalArgumentException(
                    "alias=" + fieldName + " exists in " + uniqueNameList);
        } else {
            return fieldName;
        }
    }

    /**
     * infer aliases for project or group expressions
     * @param exprList
     * @param fieldNameList
     * @param uniqueNameList
     * @return
     * @throws IllegalArgumentException - if fieldNameList has duplicated aliases or some exist in uniqueNameList
     */
    public static final List<String> inferProject(
            List<RexNode> exprList,
            List<@Nullable String> fieldNameList,
            Set<String> uniqueNameList)
            throws IllegalArgumentException {
        ObjectUtils.requireNonEmpty(exprList);
        Objects.requireNonNull(fieldNameList);
        Objects.requireNonNull(uniqueNameList);
        while (fieldNameList.size() < exprList.size()) {
            fieldNameList.add(null);
        }
        for (int i = 0; i < fieldNameList.size(); ++i) {
            if (fieldNameList.get(i) == null) {
                fieldNameList.set(i, innerInfer(exprList, exprList.get(i), i));
            } else {
                String field = fieldNameList.get(i);
                if (fieldNameList.lastIndexOf(field) != i || uniqueNameList.contains(field)) {
                    throw new IllegalArgumentException(
                            "alias="
                                    + field
                                    + " exists in "
                                    + CollectionUtils.union(fieldNameList, uniqueNameList));
                }
            }
        }
        for (int i = 0; i < fieldNameList.size(); ++i) {
            String name = fieldNameList.get(i);
            String originalName = name;
            if (name == null || uniqueNameList.contains(name)) {
                int j = 0;
                if (name == null) {
                    j = i;
                }
                do {
                    name = SqlValidatorUtil.F_SUGGESTER.apply(originalName, j, j++);
                } while (uniqueNameList.contains(name));
                fieldNameList.set(i, name);
            }
            uniqueNameList.add(name);
        }
        return fieldNameList;
    }

    /**
     * infer alias for some specific expressions,
     * i.e. a -> a, a.name -> name, a.name as b -> b
     * @param exprList
     * @param expr
     * @param i
     * @return
     */
    private static @Nullable String innerInfer(List<RexNode> exprList, RexNode expr, int i) {
        if (expr instanceof RexGraphVariable) {
            String name = ((RexGraphVariable) expr).getName();
            String[] nameArray = name.split(Pattern.quote(Static.DELIMITER));
            if (ObjectUtils.isEmpty(nameArray)) {
                return null;
            } else if (nameArray.length == 1) {
                return nameArray[0];
            } else {
                return nameArray[1];
            }
        } else {
            switch (expr.getKind()) {
                case CAST:
                    return innerInfer(exprList, ((RexCall) expr).getOperands().get(0), -1);
                case AS:
                    final RexCall call = (RexCall) expr;
                    if (i >= 0) {
                        exprList.set(i, call.getOperands().get(0));
                    }
                    NlsString value =
                            (NlsString) ((RexLiteral) call.getOperands().get(1)).getValue();
                    return castNonNull(value).getValue();
                default:
                    return null;
            }
        }
    }
}
