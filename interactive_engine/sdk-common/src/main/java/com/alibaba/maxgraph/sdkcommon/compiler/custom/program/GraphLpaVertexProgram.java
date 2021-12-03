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
package com.alibaba.maxgraph.sdkcommon.compiler.custom.program;

import com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.process.computer.*;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.List;
import java.util.Set;

public class GraphLpaVertexProgram implements CustomProgram, VertexProgram {
    public static final String TARGET_LABEL = "label";
    public static final String SEED_LABEL = "id";
    public static final int MAX_ITERATION = 20;


    private int maxIterations = MAX_ITERATION;
    private String property = TARGET_LABEL;
    private List<String> edgeLabels = Lists.newArrayList();
    private Direction direction = Direction.OUT;
    private String seed = SEED_LABEL;

    public GraphLpaVertexProgram property(String property) {
        this.property = property;
        return this;
    }

    public GraphLpaVertexProgram seed(String seed) {
        this.seed = seed;
        return this;
    }

    public GraphLpaVertexProgram edges(String... edgeLabels){
        this.edgeLabels = Lists.newArrayList(edgeLabels);
        return this;
    }

    public GraphLpaVertexProgram direction(Direction direction){
        this.direction = direction;
        return this;
    }

    public GraphLpaVertexProgram times(int maxIterations) {
        if (maxIterations <= 0) {
            throw new IllegalArgumentException("cant set iteration <= 0");
        }
        this.maxIterations = maxIterations;
        return this;
    }


    public GraphLpaVertexProgram build(){
        return this;
    }

    public String getProperty() {
        return property;
    }

    public int getMaxIterations() {
        return maxIterations;
    }

    public List<String> getEdgeLabels() { return edgeLabels; }

    public Direction getDirection() { return direction; }

    public String getSeed() { return seed; }

    @Override
    public void setup(Memory memory) {

    }

    @Override
    public void execute(Vertex vertex, Messenger messenger, Memory memory) {

    }

    @Override
    public boolean terminate(Memory memory) {
        return false;
    }

    @Override
    public Set<MessageScope> getMessageScopes(Memory memory) {
        return null;
    }

    @Override
    public VertexProgram clone() {
        return null;
    }

    @Override
    public GraphComputer.ResultGraph getPreferredResultGraph() {
        return null;
    }

    @Override
    public GraphComputer.Persist getPreferredPersist() {
        return null;
    }
}
