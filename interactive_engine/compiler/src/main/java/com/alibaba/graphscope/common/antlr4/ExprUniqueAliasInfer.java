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

package com.alibaba.graphscope.common.antlr4;

import com.google.common.collect.Sets;

import org.apache.calcite.sql.validate.SqlValidatorUtil;

import java.util.Set;

/**
 * infer a unique alias (which not contained in the uniqueNameList) for expression.
 */
public class ExprUniqueAliasInfer {
    private final Set<String> uniqueNameList;

    public ExprUniqueAliasInfer() {
        this.uniqueNameList = Sets.newHashSet();
    }

    public String infer() {
        String name;
        int j = 0;
        do {
            name = SqlValidatorUtil.EXPR_SUGGESTER.apply(null, j++, 0);
        } while (uniqueNameList.contains(name));
        uniqueNameList.add(name);
        return name;
    }
}
