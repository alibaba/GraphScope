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

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.ComparatorHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.javatuples.Pair;

import java.util.List;
import java.util.Comparator;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.ArrayList;
import java.util.stream.Collectors;

public class OrderGlobalLimitStep<S, C extends Comparable> extends AbstractStep implements ComparatorHolder<S, C>, TraversalParent {
    private List<Pair<Traversal.Admin<S, C>, Comparator<C>>> comparators = new ArrayList();
    private int limit;

    public OrderGlobalLimitStep(OrderGlobalStep step, int limit) {
        super(step.getTraversal());
        this.limit = limit;
        // this.comparators.addAll(step.getComparators());
        step.getComparators().forEach(c -> {
            this.addComparator((Traversal.Admin) ((Pair) c).getValue0(), (Comparator) ((Pair) c).getValue1());
        });
    }

    @Override
    public void addComparator(Traversal.Admin<S, C> traversal, Comparator<C> comparator) {
        this.comparators.add(new Pair(this.integrateChild(traversal), comparator));
    }

    @Override
    public List<Pair<Traversal.Admin<S, C>, Comparator<C>>> getComparators() {
        return this.comparators.isEmpty() ? Collections.singletonList(new Pair(new IdentityTraversal(), Order.asc)) : Collections.unmodifiableList(this.comparators);
    }

    @Override
    protected Traverser.Admin processNextStart() throws NoSuchElementException {
        throw new UnsupportedOperationException("");
    }

    public int getLimit() {
        return limit;
    }

    @Override
    public List<Traversal.Admin<S, C>> getLocalChildren() {
        return this.comparators.stream().map(Pair::getValue0).collect(Collectors.toList());
    }
}
