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

import com.alibaba.pegasus.builder.JobBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.Step;

public abstract class JobBuilderResource implements StepResource {
    protected abstract void buildJob(Step t, JobBuilder target);

    @Override
    public void attachResource(Step step, JobBuilder target) {
        buildJob(step, target);
        // todo: add AsStep if step tag exists
    }
}
