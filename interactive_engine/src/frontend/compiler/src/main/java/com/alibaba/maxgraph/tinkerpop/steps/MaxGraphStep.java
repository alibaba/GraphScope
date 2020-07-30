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
package com.alibaba.maxgraph.tinkerpop.steps;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Map;
import java.util.NoSuchElementException;

public class MaxGraphStep<S, E extends Element> extends GraphStep<S, E> {
    private Map<String, Object> queryConfig;

    public MaxGraphStep(Map<String, Object> queryConfig, final Traversal.Admin traversal, final Class<E> returnClass, final boolean isStart, final Object... ids) {
        super(traversal, returnClass, isStart, ids);
        this.queryConfig = queryConfig;
    }

    public Map<String, Object> getQueryConfig() {
        return queryConfig;
    }

    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        return super.processNextStart();
    }

    @Override
    public void onGraphComputer() {
        super.onGraphComputer();
    }
}
