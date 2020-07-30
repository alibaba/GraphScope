/**
 * This file is referred and derived from project apache/tinkerpop
 *
 *   https://github.com/apache/tinkerpop/blob/master/gremlin-core/src/main/java/org/apache/tinkerpop/gremlin/process/traversal/step/ByModulating.java
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
package com.alibaba.maxgraph.tinkerpop.steps;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ColumnTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ElementValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.FunctionTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TokenTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.Comparator;
import java.util.function.Function;

public class VertexWithByStep<E extends Element> extends VertexStep<E> implements ByModulating {
    private static final long serialVersionUID = -4061246771715369013L;
    private Function function = null;

    public VertexWithByStep(Traversal.Admin traversal, Class<E> returnClass, Direction direction, String... edgeLabels) {
        super(traversal, returnClass, direction, edgeLabels);
    }

    public Function getFunction() {
        return function;
    }

    public void modulateBy(final Function function) throws UnsupportedOperationException {
        this.function = function;
    }

    public void modulateBy(final String key, final Comparator comparator) {
        this.modulateBy(new ElementValueTraversal<>(key), comparator);
    }

    public void modulateBy(final Comparator comparator) {
        this.modulateBy(new IdentityTraversal<>(), comparator);
    }

    public void modulateBy(final Order order) {
        this.modulateBy(new IdentityTraversal<>(), order);
    }

    public void modulateBy(final T t, final Comparator comparator) {
        this.modulateBy(new TokenTraversal<>(t), comparator);
    }

    public void modulateBy(final Column column, final Comparator comparator) {
        this.modulateBy(new ColumnTraversal(column), comparator);
    }

    public void modulateBy(final Function function, final Comparator comparator) {
        if (function instanceof T)
            this.modulateBy((T) function, comparator);
        else if (function instanceof Column)
            this.modulateBy((Column) function, comparator);
        else
            this.modulateBy(__.map(new FunctionTraverser<>(function)).asAdmin(), comparator);
    }
}
