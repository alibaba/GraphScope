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
import org.apache.tinkerpop.gremlin.structure.Graph;

public class CustomVertexProgramStep extends VertexProgramStep {
    private static final long serialVersionUID = 2931171074623314328L;
    private VertexProgram customProgram;

    public CustomVertexProgramStep(Traversal.Admin traversal, VertexProgram customProgram) {
        super(traversal);
        this.customProgram = customProgram;
    }

    @Override
    public VertexProgram generateProgram(Graph graph, Memory memory) {
        throw new IllegalArgumentException();
    }

    public VertexProgram getCustomProgram() {
        return customProgram;
    }
}
