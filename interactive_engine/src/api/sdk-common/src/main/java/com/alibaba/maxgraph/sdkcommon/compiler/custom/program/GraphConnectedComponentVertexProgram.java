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
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.List;
import java.util.Set;

public class GraphConnectedComponentVertexProgram implements CustomProgram, VertexProgram {
    private List<String> outputList = Lists.newArrayList();
    private int iteration = 0;
    private Direction direction = Direction.BOTH;

    public GraphConnectedComponentVertexProgram output(String... list) {
        outputList.addAll(Lists.newArrayList(list));
        return this;
    }

    public GraphConnectedComponentVertexProgram iteration(int iteration) {
        if (iteration <= 0) {
            throw new IllegalArgumentException("cant set iteration <= 0");
        }
        this.iteration = iteration;
        return this;
    }

    public GraphConnectedComponentVertexProgram direction(Direction direction){
        this.direction = direction;
        return this;
    }

    public List<String> getOutputList() {
        return outputList;
    }

    public int getIteration() {
        return this.iteration;
    }

    public Direction getDirection() {
        return direction;
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
