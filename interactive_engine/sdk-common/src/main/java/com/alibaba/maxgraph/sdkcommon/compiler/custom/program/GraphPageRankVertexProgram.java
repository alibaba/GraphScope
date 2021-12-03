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

public class GraphPageRankVertexProgram implements CustomProgram, VertexProgram {

    public static final String PAGE_RANK = "pageRank";

    private double alpha = 0.85d;
    private int maxIterations = 20;
    private String property = PAGE_RANK;
    private List<String> edgeLabels = Lists.newArrayList();
    private Direction direction = Direction.OUT;

    public GraphPageRankVertexProgram property(String property) {
        this.property = property;
        return this;
    }

    public GraphPageRankVertexProgram times(int maxIterations) {
        if (maxIterations <= 0) {
            throw new IllegalArgumentException("cant set iteration <= 0");
        }
        this.maxIterations = maxIterations;
        return this;
    }

    public GraphPageRankVertexProgram alpha(int alpha){
        this.alpha=alpha;
        return this;
    }

    public GraphPageRankVertexProgram edges(String... edgeLabels){
        this.edgeLabels = Lists.newArrayList(edgeLabels);
        return this;
    }

    public GraphPageRankVertexProgram direction(Direction direction){
        this.direction = direction;
        return this;
    }

    public GraphPageRankVertexProgram build(){
        return this;
    }

    public String getProperty() {
        return property;
    }

    public double getAlpha() {
        return alpha;
    }

    public int getMaxIterations() {
        return maxIterations;
    }

    public List<String> getEdgeLabels() {
        return edgeLabels;
    }

    public Direction getDirection() {
        return direction;
    }

    public void setAlpha(double alpha) {
        this.alpha = alpha;
    }

    public void setMaxIterations(int maxIterations) {
        this.maxIterations = maxIterations;
    }

    public void setProperty(String property) {
        this.property = property;
    }

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
