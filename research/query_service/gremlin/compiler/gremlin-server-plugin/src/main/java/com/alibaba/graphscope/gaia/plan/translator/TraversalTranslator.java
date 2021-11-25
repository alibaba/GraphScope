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
package com.alibaba.graphscope.gaia.plan.translator;

import com.alibaba.pegasus.builder.AbstractBuilder;
import com.alibaba.pegasus.builder.JobBuilder;
import com.alibaba.pegasus.builder.ReduceBuilder;
import com.alibaba.pegasus.service.protocol.PegasusClient;
import com.alibaba.graphscope.gaia.plan.strategy.DummyStep;
import com.alibaba.graphscope.gaia.plan.translator.builder.PlanConfig;
import com.alibaba.graphscope.gaia.plan.translator.builder.StepBuilder;
import com.alibaba.graphscope.gaia.plan.translator.builder.TraversalBuilder;
import com.google.protobuf.ByteString;
import org.apache.tinkerpop.gremlin.process.traversal.Step;

import java.util.*;
import java.util.function.Function;

public class TraversalTranslator extends AttributeTranslator<TraversalBuilder, AbstractBuilder> {
    public TraversalTranslator(TraversalBuilder traversalBuilder) {
        super(traversalBuilder);
    }

    @Override
    protected Function<TraversalBuilder, AbstractBuilder> getApplyFunc() {
        return (TraversalBuilder t) -> {
            AbstractBuilder builder = new JobBuilder();
            builder.setConf((PegasusClient.JobConfig) t.getConf().getProperty(PlanConfig.QUERY_CONFIG));
            List<Step> steps = t.getAdmin().getSteps();
            for (Step s : steps) {
                if (s instanceof DummyStep) continue;
                StepBuilder stepBuilder = (new StepBuilder(s, builder)).setConf(t.getConf());
                (new StepTranslator(stepBuilder)).translate();
                builder = stepBuilder.getJobBuilder();
            }
            if (builder instanceof ReduceBuilder) {
                builder = ((ReduceBuilder) builder).unfold(ByteString.EMPTY);
            }
            return builder;
        };
    }
}
