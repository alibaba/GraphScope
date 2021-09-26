/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.edge;

import org.apache.hadoop.io.LongWritable;

/**
 * CAUTION: do not keep the returned value in context, since it is reusable object.
 */
public class LongLongEdge implements ReusableEdge<LongWritable, LongWritable> {

    private LongWritable targetId;
    private LongWritable edgeValue;

    public LongLongEdge(long id, long value) {
        this.targetId = new LongWritable(id);
        this.edgeValue = new LongWritable(value);
    }

    public LongLongEdge(LongWritable id, LongWritable value) {
        this.targetId = new LongWritable(id.get());
        this.edgeValue = new LongWritable(value.get());
    }

    public LongLongEdge() {
        targetId = new LongWritable(0);
        edgeValue = new LongWritable(0);
    }

    /**
     * Get the target vertex index of this edge
     *
     * @return Target vertex index of this edge
     */
    @Override
    public LongWritable getTargetVertexId() {
        return targetId;
    }

    /**
     * Set the destination vertex index of this edge.
     *
     * @param targetVertexId new destination vertex
     */
    @Override
    public void setTargetVertexId(LongWritable targetVertexId) {
        this.targetId.set(targetVertexId.get());
    }

    public void setTargetVertexId(long id) {
        this.targetId.set(id);
    }

    /**
     * Get the edge value of the edge
     *
     * @return Edge value of this edge
     */
    @Override
    public LongWritable getValue() {
        return edgeValue;
    }

    /**
     * Set the value for this edge.
     *
     * @param value new edge value
     */
    @Override
    public void setValue(LongWritable value) {
        this.edgeValue.set(value.get());
    }

    public void setValue(Long value) {
        this.edgeValue.set(value);
    }
}
