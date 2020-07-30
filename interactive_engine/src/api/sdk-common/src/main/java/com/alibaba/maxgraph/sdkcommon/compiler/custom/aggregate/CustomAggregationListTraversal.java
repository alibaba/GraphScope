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
package com.alibaba.maxgraph.sdkcommon.compiler.custom.aggregate;

import com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.AbstractLambdaTraversal;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class CustomAggregationListTraversal<T, R> extends AbstractLambdaTraversal<Traverser<T>, R> {
    private List<Traversal<?, ?>> traversalList;
    private List<String> nameList;

    public CustomAggregationListTraversal(List<Traversal<?, ?>> traversalList) {
        this.traversalList = traversalList;
    }

    public CustomAggregationListTraversal as(String... nameList) {
        checkArgument(nameList.length == traversalList.size(), "length of name list must equal to traversal list");
        this.nameList = Lists.newArrayList(nameList);
        return this;
    }

    public List<String> getNameList() {
        return this.nameList;
    }

    public List<Traversal<?, ?>> getTraversalList() {
        return this.traversalList;
    }
}
