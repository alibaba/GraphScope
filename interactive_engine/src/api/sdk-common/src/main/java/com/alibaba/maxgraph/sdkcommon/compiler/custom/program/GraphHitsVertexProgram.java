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
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.List;
import java.util.Set;

public class GraphHitsVertexProgram implements CustomProgram, VertexProgram {
    public static final String AUTH_SCORE = "auth";
    public static final String HUB_SCORE = "hub";
    public static final int MAX_ITERATION = 3;

    private int maxIterations = MAX_ITERATION;
    private String authProp = AUTH_SCORE;
    private String hubProp = HUB_SCORE;
    private List<String> edgeLabels = Lists.newArrayList();

    public GraphHitsVertexProgram authProp(String property) {
        this.authProp = property;
        return this;
    }

    public GraphHitsVertexProgram hubProp(String property){
        this.hubProp = property;
        return this;
    }

    public GraphHitsVertexProgram times(int maxIterations) {
        if (maxIterations <= 0) {
            throw new IllegalArgumentException("cant set iteration <= 0");
        }
        this.maxIterations = maxIterations;
        return this;
    }

    public GraphHitsVertexProgram edges(String... edgeLabels) {
        this.edgeLabels = Lists.newArrayList(edgeLabels);
        return this;
    }

    public GraphHitsVertexProgram build(){
        return this;
    }

    public String getAuthProp() {
        return authProp;
    }

    public String getHubProp() { return hubProp; }

    public int getMaxIterations() {
        return maxIterations;
    }

    public List<String> getEdgeLabels() {
        return edgeLabels;
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
