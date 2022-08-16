/*
 * This file is referred and derived from project apache/tinkerpop
 *
 * https://github.com/apache/tinkerpop/blob/master/gremlin-core/src/main/java/org/apache/tinkerpop/gremlin/process/traversal/step/map/GroupCountStep.java
 *
 * which has the following license:
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.alibaba.graphscope.gremlin.plugin.step;

import com.google.common.base.Objects;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal.Admin;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.*;

// rewrite GroupCount to enable multiple group keys
public class GroupCountStep<S, E> extends ReducingBarrierStep<S, Map<E, Long>>
        implements TraversalParent, ByModulating, MultiByModulating {
    private List<Admin<S, E>> keyTraversalList;

    public GroupCountStep(Traversal.Admin traversal) {
        super(traversal);
        this.keyTraversalList = new ArrayList<>();
    }

    // by single key
    @Override
    public void modulateBy(final Traversal.Admin<?, ?> keyTraversal) {
        this.keyTraversalList.add(this.integrateChild(keyTraversal));
    }

    // by multiple keys
    @Override
    public void modulateBy(final List<Admin<?, ?>> kvTraversals) {
        kvTraversals.forEach(k -> this.keyTraversalList.add(this.integrateChild(k)));
    }

    @Override
    public List<Traversal.Admin<S, E>> getLocalChildren() {
        return Collections.unmodifiableList(this.keyTraversalList);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(
                new TraverserRequirement[] {TraverserRequirement.BULK});
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, new Object[] {this.keyTraversalList});
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        GroupCountStep<?, ?> that = (GroupCountStep<?, ?>) o;
        return Objects.equal(keyTraversalList, that.keyTraversalList);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), keyTraversalList);
    }

    @Override
    public Map<E, Long> projectTraverser(final Traverser.Admin<S> traverser) {
        throw new UnsupportedOperationException(
                "project traverser shoule be implemented in runtime but unnecessary in compiler");
    }
}
