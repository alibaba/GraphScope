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
package com.alibaba.maxgraph.compiler.prepare;

import com.alibaba.maxgraph.structure.GraphPrepare;
import com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

import java.util.List;

public class DefaultGraphPrepare implements GraphPrepare {
    @Override
    public PreparedTraversal prepare(String prepareId, GraphTraversal traversal) {
        return new PreparedTraversal(prepareId, traversal);
    }

    @Override
    public PreparedExecuteParam execute(String prepareId, List<Object> ... paramList) {
        return new PreparedExecuteParam(prepareId, Lists.newArrayList(paramList));
    }

    @Override
    public List<Object> param(Object... paramValue) {
        return Lists.newArrayList(paramValue);
    }
}
