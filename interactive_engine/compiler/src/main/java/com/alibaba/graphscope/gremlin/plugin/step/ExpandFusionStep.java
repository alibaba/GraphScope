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

package com.alibaba.graphscope.gremlin.plugin.step;

import com.alibaba.graphscope.common.jna.type.FfiExpandOpt;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ExpandFusionStep<E extends Element> extends VertexStep<E>
        implements HasContainerHolder {
    private final List<HasContainer> hasContainers = new ArrayList<>();
    private FfiExpandOpt expandOpt;

    public ExpandFusionStep(
            final Traversal.Admin traversal,
            final Class<E> returnClass,
            final Direction direction,
            final String... edgeLabels) {
        super(traversal, returnClass, direction, edgeLabels);
        this.expandOpt = returnsVertex() ? FfiExpandOpt.Vertex : FfiExpandOpt.Edge;
    }

    public ExpandFusionStep(VertexStep<E> originalVertexStep) {
        this(
                originalVertexStep.getTraversal(),
                originalVertexStep.getReturnClass(),
                originalVertexStep.getDirection(),
                originalVertexStep.getEdgeLabels());
        originalVertexStep.getLabels().forEach(this::addLabel);
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return Collections.unmodifiableList(this.hasContainers);
    }

    @Override
    public void addHasContainer(HasContainer hasContainer) {
        if (hasContainer.getPredicate() instanceof AndP) {
            for (final P<?> predicate : ((AndP<?>) hasContainer.getPredicate()).getPredicates()) {
                this.addHasContainer(new HasContainer(hasContainer.getKey(), predicate));
            }
        } else this.hasContainers.add(hasContainer);
    }

    public void setExpandOpt(FfiExpandOpt opt) {
        this.expandOpt = opt;
    }

    public FfiExpandOpt getExpandOpt() {
        return this.expandOpt;
    }
}
