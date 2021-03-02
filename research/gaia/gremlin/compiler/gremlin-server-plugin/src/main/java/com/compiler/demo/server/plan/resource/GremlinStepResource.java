/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.compiler.demo.server.plan.resource;

import com.alibaba.graphscope.common.proto.Gremlin;
import com.alibaba.graphscope.common.proto.Gremlin.GremlinStep;
import com.alibaba.pegasus.builder.JobBuilder;
import com.compiler.demo.server.plan.strategy.shuffle.ShuffleStrategy;
import com.google.protobuf.ByteString;
import org.apache.tinkerpop.gremlin.process.traversal.Step;

public abstract class GremlinStepResource implements StepResource {
    protected abstract Object getStepResource(Step t);

    @Override
    public void attachResource(Step step, JobBuilder target) {
        if (ShuffleStrategy.needShuffle(step)) {
            target.exchange(ByteString.EMPTY);
        }
        addGremlinStep(step, target);
    }

    public static GremlinStep.Builder createResourceBuilder(Step t) {
        GremlinStep.Builder builder = GremlinStep.newBuilder();
        if (!t.getLabels().isEmpty()) {
            builder.addAllTags(t.getLabels());
        }
        return builder;
    }

    protected void addGremlinStep(Step t, JobBuilder target) {
        GremlinStep.Builder builder = createResourceBuilder(t);
        Object stepResurce = getStepResource(t);
        if (stepResurce instanceof Gremlin.GraphStep) {
            builder.setGraphStep((Gremlin.GraphStep) stepResurce);
            target.addSource(builder.build().toByteString());
        } else if (stepResurce instanceof Gremlin.VertexStep) {
            builder.setVertexStep((Gremlin.VertexStep) stepResurce);
            target.flatMap(builder.build().toByteString());
        } else if (stepResurce instanceof Gremlin.HasStep) {
            builder.setHasStep((Gremlin.HasStep) stepResurce);
            target.filter(builder.build().toByteString());
        } else if (stepResurce instanceof Gremlin.WhereStep) {
            builder.setWhereStep((Gremlin.WhereStep) stepResurce);
            target.filter(builder.build().toByteString());
        } else if (stepResurce instanceof Gremlin.PathStep) {
            builder.setPathStep((Gremlin.PathStep) stepResurce);
            target.map(builder.build().toByteString());
        } else if (stepResurce instanceof Gremlin.PathFilterStep) {
            builder.setPathFilterStep((Gremlin.PathFilterStep) stepResurce);
            target.filter(builder.build().toByteString());
        } else if (stepResurce instanceof Gremlin.IdentityStep) {
            builder.setIdentityStep((Gremlin.IdentityStep) stepResurce);
            target.map(builder.build().toByteString());
        } else if (stepResurce instanceof Gremlin.SelectStep) {
            builder.setSelectStep((Gremlin.SelectStep) stepResurce);
            target.map(builder.build().toByteString());
        } else if (stepResurce instanceof Gremlin.ValuesStep) {
            builder.setValuesStep((Gremlin.ValuesStep) stepResurce);
            target.flatMap(builder.build().toByteString());
        } else if (stepResurce instanceof Gremlin.PathLocalCountStep) {
            builder.setPathLocalCountStep((Gremlin.PathLocalCountStep) stepResurce);
            target.map(builder.build().toByteString());
        } else if (stepResurce instanceof Gremlin.EdgeVertexStep) {
            builder.setEdgeVertexStep((Gremlin.EdgeVertexStep) stepResurce);
            target.flatMap(builder.build().toByteString());
        } else {
            throw new UnsupportedOperationException("operator " + t.getClass() + " not implemented");
        }
    }
}
