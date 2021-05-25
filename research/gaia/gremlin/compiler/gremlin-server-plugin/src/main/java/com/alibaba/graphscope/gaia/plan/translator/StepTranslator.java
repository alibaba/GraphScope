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

import com.alibaba.graphscope.gaia.plan.LogicPlanGlobalMap;
import com.alibaba.graphscope.gaia.plan.resource.StepResource;
import com.alibaba.graphscope.gaia.plan.translator.builder.StepBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.Function;

public class StepTranslator extends AttributeTranslator<StepBuilder, Void> {
    private static Logger logger = LoggerFactory.getLogger(StepTranslator.class);

    public StepTranslator(StepBuilder stepBuilder) {
        super(stepBuilder);
    }

    @Override
    protected Function<StepBuilder, Void> getApplyFunc() {
        return (StepBuilder stepBuilder) -> {
            Step step = stepBuilder.getStep();
            Optional<StepResource> constructorOpt = LogicPlanGlobalMap.getResourceConstructor(LogicPlanGlobalMap.stepType(step));
            if (constructorOpt.isPresent()) {
                constructorOpt.get().attachResource(stepBuilder);
            } else {
                throw new UnsupportedOperationException("compiler not support StepResource of step " + step.getClass());
            }
            return null;
        };
    }
}
