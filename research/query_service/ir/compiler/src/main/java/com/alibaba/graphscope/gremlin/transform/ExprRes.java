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

package com.alibaba.graphscope.gremlin.transform;

import java.util.*;

public class ExprRes {
    // if all of the by_traversals can be converted to expressions, return true
    // otherwise false
    private boolean isExprPattern;
    // store the tag and the corresponding by_traversal as expression if it can be converted
    // especially, tag is "" if no tag exists, i.e. values(..), valueMap(..)
    private Map<String, String> tagExprMap;

    public ExprRes() {
        this.isExprPattern = false;
        this.tagExprMap = new LinkedHashMap<>();
    }

    public ExprRes(boolean isExprPattern) {
        this.tagExprMap = new LinkedHashMap<>();
        this.isExprPattern = isExprPattern;
    }

    public List<String> getExprs() {
        return new ArrayList<>(tagExprMap.values());
    }

    public Optional<String> getSingleExpr() {
        List<String> exprs = getExprs();
        return exprs.isEmpty() ? Optional.empty() : Optional.of(exprs.get(0));
    }

    public Optional<String> getTagExpr(String tag) {
        String expr = tagExprMap.get(tag);
        return (expr == null || expr.isEmpty()) ? Optional.empty() : Optional.of(expr);
    }

    public boolean isExprPattern() {
        return this.isExprPattern;
    }

    public ExprRes addTagExpr(String tag, String expr) {
        tagExprMap.put(tag, expr);
        return this;
    }

    public ExprRes setExprPattern(boolean exprPattern) {
        isExprPattern = exprPattern;
        return this;
    }
}
