/*
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
package com.alibaba.graphscope.gaia.plan.resource;

import com.alibaba.graphscope.common.proto.Gremlin;
import com.alibaba.graphscope.common.proto.Gremlin.GremlinStep;
import com.alibaba.graphscope.gaia.idmaker.IdMaker;
import com.alibaba.graphscope.gaia.plan.strategy.global.RemovePathHistoryStep;
import com.alibaba.pegasus.builder.AbstractBuilder;
import com.alibaba.pegasus.builder.JobBuilder;
import com.alibaba.graphscope.gaia.plan.PlanUtils;
import com.alibaba.graphscope.gaia.plan.strategy.shuffle.ShuffleStrategy;
import com.alibaba.graphscope.gaia.plan.translator.builder.StepBuilder;
import com.google.protobuf.ByteString;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.Step;

public abstract class GremlinStepResource implements StepResource {
    protected abstract Object getStepResource(Step t, Configuration conf);

    @Override
    public void attachResource(StepBuilder stepBuilder) {
        Step step = stepBuilder.getStep();
        AbstractBuilder target = stepBuilder.getJobBuilder();
        if (ShuffleStrategy.needShuffle(step)) {
            ((JobBuilder) target).exchange(ByteString.EMPTY);
        }
        addGremlinStep(stepBuilder);
    }

    public static GremlinStep.Builder createResourceBuilder(Step t, Configuration conf) {
        GremlinStep.Builder builder = GremlinStep.newBuilder();
        IdMaker tagIdMaker = PlanUtils.getTagIdMaker(conf);
        if (!t.getLabels().isEmpty()) {
            t.getLabels().forEach(k -> builder.addTags(Gremlin.StepTag.newBuilder().setTag((int) tagIdMaker.getId(k))));
        }
        if (t.getNextStep() instanceof RemovePathHistoryStep) {
            RemovePathHistoryStep removeStep = (RemovePathHistoryStep) t.getNextStep();
            if (removeStep.getRemoveTags() != null) {
                removeStep.getRemoveTags().forEach(k -> {
                    builder.addRemoveTags(Gremlin.StepTag.newBuilder().setTag((int) tagIdMaker.getId(k)));
                });
            }
        }
        return builder;
    }

    protected void addGremlinStep(StepBuilder stepBuilder) {
        Step t = stepBuilder.getStep();
        JobBuilder target = (JobBuilder) stepBuilder.getJobBuilder();
        GremlinStep.Builder builder = createResourceBuilder(t, stepBuilder.getConf());
        Object stepResurce = getStepResource(t, stepBuilder.getConf());
        if (stepResurce instanceof Gremlin.GraphStep) {
            builder.setGraphStep((Gremlin.GraphStep) stepResurce);
            target.addSource(builder.build().toByteString());
        } else if (stepResurce instanceof Gremlin.VertexStep) {
            builder.setVertexStep((Gremlin.VertexStep) stepResurce);
            target.flatMap(builder.build().toByteString());
        } else if (stepResurce instanceof Gremlin.PathStep) {
            builder.setPathStep((Gremlin.PathStep) stepResurce);
            target.map(builder.build().toByteString());
        } else if (stepResurce instanceof Gremlin.IdentityStep) {
            builder.setIdentityStep((Gremlin.IdentityStep) stepResurce);
            target.map(builder.build().toByteString());
        } else if (stepResurce instanceof Gremlin.SelectStep) {
            builder.setSelectStep((Gremlin.SelectStep) stepResurce);
            target.map(builder.build().toByteString());
        } else if (stepResurce instanceof Gremlin.PropertiesStep) {
            builder.setPropertiesStep((Gremlin.PropertiesStep) stepResurce);
            target.flatMap(builder.build().toByteString());
        } else if (stepResurce instanceof Gremlin.PathLocalCountStep) {
            builder.setPathLocalCountStep((Gremlin.PathLocalCountStep) stepResurce);
            target.map(builder.build().toByteString());
        } else if (stepResurce instanceof Gremlin.EdgeVertexStep) {
            builder.setEdgeVertexStep((Gremlin.EdgeVertexStep) stepResurce);
            target.map(builder.build().toByteString());
        } else if (stepResurce instanceof Gremlin.UnfoldStep) {
            builder.setUnfoldStep((Gremlin.UnfoldStep) stepResurce);
            target.flatMap(builder.build().toByteString());
        } else if (stepResurce instanceof Gremlin.EdgeBothVStep) {
            builder.setEdgeBothVStep((Gremlin.EdgeBothVStep) stepResurce);
            target.flatMap(builder.build().toByteString());
        } else if (stepResurce instanceof Gremlin.SelectStep) {
            builder.setSelectStep((Gremlin.SelectStep) stepResurce);
            target.map(builder.build().toByteString());
        } else if (stepResurce instanceof Gremlin.SelectOneStepWithoutBy) {
            builder.setSelectOneWithoutBy((Gremlin.SelectOneStepWithoutBy) stepResurce);
            target.map(builder.build().toByteString());
        } else if (stepResurce instanceof Gremlin.TransformTraverserStep) {
            builder.setTransformTraverserStep((Gremlin.TransformTraverserStep) stepResurce);
            target.map(builder.build().toByteString());
        } else {
            throw new UnsupportedOperationException("operator " + t.getClass() + " not implemented");
        }
    }
}
