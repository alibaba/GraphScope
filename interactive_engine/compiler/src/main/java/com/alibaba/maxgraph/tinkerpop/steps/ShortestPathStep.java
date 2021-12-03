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
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

public class ShortestPathStep<E extends Element> extends FlatMapStep<Vertex, E> {
    public final Long sid;
    public final Long tid;
    public final String sidPropId;
    public final String outPropId;
    public final String edgeWeightPropId;
    public final int iteration;

    /**
     * single source shortest path
     */
    public ShortestPathStep(Traversal.Admin traversal, Long sid, String outPropId, String sidPropId, int iteration) {
        super(traversal);
        this.sid = sid;
        this.tid = -1L;
        this.sidPropId = sidPropId;
        this.outPropId = outPropId;
        this.edgeWeightPropId = null;
        this.iteration = iteration;
    }

    /**
     * single source shortest path on weighted graph
     */
    public ShortestPathStep(Traversal.Admin traversal, Long sid, String edgePropId, String outPropId, String sidPropId, int iteration) {
        super(traversal);
        this.sid = sid;
        this.tid = -1L;
        this.sidPropId = sidPropId;
        this.outPropId = outPropId;
        this.edgeWeightPropId = edgePropId;
        this.iteration = iteration;
    }

    /**
     * s-t pair shortest path
     */
    public ShortestPathStep(Traversal.Admin traversal, Long sid, Long tid, String outPropId, String sidPropId, int iteration) {
        super(traversal);
        this.sid = sid;
        this.sidPropId = sidPropId;
        this.tid = tid;
        this.outPropId = outPropId;
        this.edgeWeightPropId = null;
        this.iteration = iteration;
    }

    /**
     * all pair shortest path
     */
    public ShortestPathStep(Traversal.Admin traversal, String outPropId, String sidPropId, int iteration) {
        super(traversal);
        this.sid = -1L;
        this.tid = -1L;
        this.sidPropId = sidPropId;
        this.outPropId = outPropId;
        this.edgeWeightPropId = null;
        this.iteration = iteration;
    }

    /**
     * all pair shortest path on weighted graph
     */
    public ShortestPathStep(Traversal.Admin traversal, String edgePropId, String outPropId, String sidPropId, int iteration) {
        super(traversal);
        this.sid = -1L;
        this.tid = -1L;
        this.sidPropId = sidPropId;
        this.outPropId = outPropId;
        this.edgeWeightPropId = edgePropId;
        this.iteration = iteration;
    }

    @Override
    protected Iterator<E> flatMap(Traverser.Admin<Vertex> traverser) {
        throw new IllegalArgumentException();
    }
}
