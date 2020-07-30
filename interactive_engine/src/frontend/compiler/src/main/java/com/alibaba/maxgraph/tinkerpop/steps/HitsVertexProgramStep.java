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
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.VertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.List;

public class HitsVertexProgramStep extends VertexProgramStep implements TraversalParent, Configuring {

    public static final String AUTH_SCORE = "auth";
    public static final String HUB_SCORE = "hub";
    public static final int MAX_ITERATION = 3;

    private Parameters parameters = new Parameters();
    private int maxIterations = MAX_ITERATION;
    private String authProp = AUTH_SCORE;
    private String hubProp = HUB_SCORE;
    private List<String> edgeLabels = Lists.newArrayList();

    public HitsVertexProgramStep(Traversal.Admin traversal) {
        super(traversal);
    }

    public String getAuthProp() { return authProp; }

    public String getHubProp() { return hubProp; }

    public int getMaxIterations() {
        return maxIterations;
    }

    public List<String> getEdgeLabels() {
        return edgeLabels;
    }

    @Override
    public void configure(Object... keyValues) {
        Object key = keyValues[0];
        Object value = keyValues[1];
        if (key.equals(Hits.AUTH_PROP)) {
            if (!(value instanceof String)) {
                throw new IllegalArgumentException("auth requires a String as its argument");
            }
            this.authProp = (String)value;
        }
        else if (key.equals(Hits.HUB_PROP)) {
            if (!(value instanceof String)) {
                throw new IllegalArgumentException("hub requires a String as its argument");
            }
            this.hubProp = (String)value;
        }
        else if (key.equals(Hits.TIMES)) {
            if (!(value instanceof Integer)) {
                throw new IllegalArgumentException("times requires an Integer as its argument");
            }
            this.maxIterations = (int)value;
        } else if (key.equals(Hits.EDGES)) {
            if (!(value instanceof String)) {
                throw new IllegalArgumentException("edges requires an String as its argument");
            }
            this.edgeLabels = Lists.newArrayList((String)value);
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
