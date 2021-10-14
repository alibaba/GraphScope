/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.tinkerpop.steps;

import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.VertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collections;
import java.util.List;

public class LpaVertexProgramStep extends VertexProgramStep
        implements TraversalParent, Configuring {

    public static final String TARGET_LABEL = "label";
    public static final String SEED_LABEL = "id";
    public static final int MAX_ITERATION = 20;

    private Parameters parameters = new Parameters();
    private int maxIterations = MAX_ITERATION;
    private String property = TARGET_LABEL;
    private String label = SEED_LABEL;
    private PureTraversal<Vertex, Edge> edgeTraversal;

    public LpaVertexProgramStep(Traversal.Admin traversal) {
        super(traversal);
    }

    public String getProperty() {
        return property;
    }

    public int getMaxIterations() {
        return maxIterations;
    }

    public String getLabel() {
        return label;
    }

    @Override
    public List<Traversal.Admin<Vertex, Edge>> getLocalChildren() {
        return Collections.singletonList(this.edgeTraversal.get());
    }

    @Override
    public void configure(Object... keyValues) {
        Object key = keyValues[0];
        Object value = keyValues[1];
        if (key.equals(Lpa.PROPERTY_NAME)) {
            if (!(value instanceof String)) {
                throw new IllegalArgumentException("property requires a String as its argument");
            }
            this.property = (String) value;
        } else if (key.equals(Lpa.SEED_PROPERTY)) {
            if (!(value instanceof String)) {
                throw new IllegalArgumentException("label requires a String as its argument");
            }
            this.label = (String) value;
        } else if (key.equals(Lpa.TIMES)) {
            if (!(value instanceof Integer)) {
                throw new IllegalArgumentException("times requires an Integer as its argument");
            }
            this.maxIterations = (int) value;
        }
        if (key.equals(Lpa.EDGES)) {
            if (!(value instanceof Traversal)) {
                throw new IllegalArgumentException(
                        "edges in LPA requires an String as its argument");
            }
            this.edgeTraversal = new PureTraversal<>(((Traversal<Vertex, Edge>) value).asAdmin());
            this.integrateChild(this.edgeTraversal.get());
        } else {
            this.parameters.set(this, keyValues);
        }
    }

    @Override
    public Parameters getParameters() {
        return parameters;
    }

    @Override
    public VertexProgram generateProgram(Graph graph, Memory memory) {
        throw new IllegalArgumentException();
    }
}
