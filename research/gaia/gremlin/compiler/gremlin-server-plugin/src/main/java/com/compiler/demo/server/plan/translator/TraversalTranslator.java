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
package com.compiler.demo.server.plan.translator;

import com.alibaba.pegasus.builder.JobBuilder;
import com.compiler.demo.server.plan.PlanUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

import java.util.*;
import java.util.function.Function;

public class TraversalTranslator extends AttributeTranslator<Traversal.Admin, JobBuilder> {
    public TraversalTranslator(Traversal.Admin admin) {
        super(admin);
    }

    @Override
    protected Function<Traversal.Admin, JobBuilder> getApplyFunc() {
        return (Traversal.Admin t) -> {
            JobBuilder builder = new JobBuilder();
            builder.setConf(PlanUtils.getDefaultConfig());
            List<Step> steps = t.getSteps();
            for (Step s : steps) {
                StepBuilder stepBuilder = new StepBuilder(s, builder);
                (new StepTranslator(stepBuilder)).translate();
            }
            return builder;
        };
    }
}
