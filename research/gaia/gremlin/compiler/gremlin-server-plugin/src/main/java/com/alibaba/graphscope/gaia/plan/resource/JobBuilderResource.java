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
import com.alibaba.graphscope.gaia.plan.PlanUtils;
import com.alibaba.graphscope.gaia.plan.strategy.global.property.cache.ToFetchProperties;
import com.alibaba.graphscope.gaia.plan.translator.builder.StepBuilder;
import com.alibaba.pegasus.builder.AbstractBuilder;
import com.alibaba.pegasus.builder.JobBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.Step;

import java.util.Collections;

public abstract class JobBuilderResource implements StepResource {
    protected abstract void buildJob(StepBuilder stepBuilder);

    @Override
    public void attachResource(StepBuilder stepBuilder) {
        AbstractBuilder target = stepBuilder.getJobBuilder();
        Step step = stepBuilder.getStep();
        buildJob(stepBuilder);
        if (!step.getLabels().isEmpty() && target instanceof JobBuilder) {
            // do nothing just as(tag)
            Gremlin.QueryParams params = Gremlin.QueryParams.newBuilder()
                    .setRequiredProperties(PlanUtils.convertFrom(new ToFetchProperties(false, Collections.EMPTY_LIST)))
                    .build();
            Gremlin.IdentityStep.Builder identityStep = Gremlin.IdentityStep.newBuilder()
                    .setQueryParams(params);
            ((JobBuilder) target).map(GremlinStepResource.createResourceBuilder(step, stepBuilder.getConf())
                    .setIdentityStep(identityStep).build().toByteString());

        }
    }
}
