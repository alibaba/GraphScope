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
package com.alibaba.graphscope.gaia.plan.strategy.global;

import com.alibaba.graphscope.gaia.plan.strategy.DummyStep;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.NoSuchElementException;
import java.util.Set;

public class RemovePathHistoryStep<S, E extends Element> extends DummyStep<S, E> {
    private Set<String> removeTags;

    public RemovePathHistoryStep(Traversal.Admin traversal, Set<String> removeTags) {
        super(traversal);
        this.removeTags = removeTags;
    }

    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        throw new UnsupportedOperationException();
    }

    public Set<String> getRemoveTags() {
        return removeTags;
    }
}
