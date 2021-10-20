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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.GraphComputing;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.NoSuchElementException;
import java.util.Set;

public class EstimateCountStep<S, E extends Element> extends AbstractStep<S, E> implements GraphComputing {
    private Set<String> labelList;
    private boolean vertexFlag;

    public EstimateCountStep(Traversal.Admin traversal, boolean vertexFlag, Set<String> labelList) {
        super(traversal);
        this.vertexFlag = vertexFlag;
        this.labelList = labelList;
    }

    public boolean isVertexFlag() {
        return this.vertexFlag;
    }

    public Set<String> getLabelList() {
        return labelList;
    }

    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onGraphComputer() {
        throw new UnsupportedOperationException();
    }
}
