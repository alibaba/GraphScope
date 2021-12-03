/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.pegasus.builder;

import com.alibaba.pegasus.service.protocol.PegasusClient.Sink;
import com.alibaba.pegasus.service.protocol.PegasusClient.JobConfig;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReduceBuilder extends AbstractBuilder {
    private static final Logger logger = LoggerFactory.getLogger(ReduceBuilder.class);

    public ReduceBuilder(JobConfig conf, ByteString source, Plan plan, Sink sink) {
        super(conf, source, plan, sink);
    }

    public JobBuilder unfold(ByteString func) {
        this.plan.chainUnfold(func);
        return new JobBuilder(this.conf, this.source, this.plan, this.sink);
    }
}
