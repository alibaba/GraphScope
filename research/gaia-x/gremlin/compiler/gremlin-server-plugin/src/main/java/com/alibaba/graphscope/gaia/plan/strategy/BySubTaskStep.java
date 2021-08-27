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
package com.alibaba.graphscope.gaia.plan.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

public class BySubTaskStep<S, E extends Element> extends AbstractStep<S, E> implements TraversalParent {
    public enum JoinerType {
        Select,
        GroupKeyBy,
        GroupValueBy,
        OrderBy
    }

    private JoinerType joiner;
    private Traversal.Admin bySubTraversal;

    public BySubTaskStep(Traversal.Admin traversal, Traversal.Admin bySubTraversal, JoinerType joiner) {
        super(traversal);
        this.joiner = joiner;
        this.bySubTraversal = bySubTraversal;
        this.bySubTraversal.setParent(this);
    }

    public JoinerType getJoiner() {
        return this.joiner;
    }

    public Traversal.Admin getBySubTraversal() {
        return this.bySubTraversal;
    }

    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Traversal.Admin<S, ?>> getLocalChildren() {
        return Collections.singletonList(this.bySubTraversal);
    }
}
