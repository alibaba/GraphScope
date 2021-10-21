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

import com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;
import java.util.List;

public class RatioVertexStep<E extends Element> extends FlatMapStep<Vertex, E> {
    private P predicate;
    private Direction direction;
    private List<String> labelList;

    public RatioVertexStep(Traversal.Admin traversal, P predicate, Direction direction, String[] labels) {
        super(traversal);
        this.predicate = predicate;
        this.direction = direction;
        if (null == labels) {
            this.labelList = Lists.newArrayList();
        } else {
            this.labelList = Lists.newArrayList(labels);
        }
    }

    public P getPredicate() {
        return predicate;
    }

    public Direction getDirection() {
        return direction;
    }

    public List<String> getLabelList() {
        return labelList;
    }

    @Override
    protected Iterator<E> flatMap(Traverser.Admin<Vertex> traverser) {
        throw new IllegalArgumentException();
    }
}
