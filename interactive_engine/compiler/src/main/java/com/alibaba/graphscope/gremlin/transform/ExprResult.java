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

public class ExprResult {
    // store the tag and the corresponding by_traversal as expression if it can be converted,
    // otherwise is Optional.empty
    // especially, tag is "" if no tag exists, i.e. values(..), valueMap(..)
    private Map<String, Optional<String>> tagExprMap;

    public ExprResult() {
        this.tagExprMap = new LinkedHashMap<>();
    }

    public Map<String, Optional<String>> getTagExprMap() {
        return Collections.unmodifiableMap(tagExprMap);
    }

    public ExprResult addTagExpr(String tag, Optional<String> exprOpt) {
        this.tagExprMap.put(tag, exprOpt);
        return this;
    }

    // return the first expression in tagExprMap
    public Optional<String> getSingleExpr() {
        return this.tagExprMap.values().iterator().next();
    }
}
