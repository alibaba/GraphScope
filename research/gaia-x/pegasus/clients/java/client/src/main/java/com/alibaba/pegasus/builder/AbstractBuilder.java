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
import com.alibaba.pegasus.service.protocol.PegasusClient.TaskPlan;
import com.alibaba.pegasus.service.protocol.PegasusClient.Source;
import com.alibaba.pegasus.service.protocol.PegasusClient.JobConfig;
import com.alibaba.pegasus.service.protocol.PegasusClient.JobRequest;
import com.google.protobuf.ByteString;

public abstract class AbstractBuilder {
    protected JobConfig conf;
    protected ByteString source;
    protected Plan plan;
    protected Sink sink;

    public AbstractBuilder(JobConfig conf, ByteString source, Plan plan, Sink sink) {
        this.conf = conf;
        this.source = source;
        this.plan = plan;
        this.sink = sink;
    }

    public AbstractBuilder(JobConfig conf, ByteString source, Plan plan) {
        this.conf = conf;
        this.source = source;
        this.plan = plan;
        this.sink = Sink.newBuilder().build();
    }

    public AbstractBuilder(JobConfig conf, ByteString source) {
        this.conf = conf;
        this.source = source;
        this.plan = new Plan();
        this.sink = Sink.newBuilder().build();
    }

    public AbstractBuilder(JobConfig conf) {
        this.conf = conf;
        this.plan = new Plan();
        this.sink = Sink.newBuilder().build();
    }

    public AbstractBuilder() {
        this.plan = new Plan();
        this.sink = Sink.newBuilder().build();
    }

    public JobConfig getConf() {
        return conf;
    }

    public void setConf(JobConfig conf) {
        this.conf = conf;
    }

    public ByteString getSource() {
        return source;
    }

    public Plan getPlan() {
        return plan;
    }

    public void setPlan(Plan plan) {
        this.plan = plan;
    }

    public JobRequest build() {
        Sink sink = this.sink;
        if (this.plan.endReduce()) {
            sink = this.plan.genSink();
        }
        return JobRequest.newBuilder()
                .setConf(this.conf)
                .setSource(Source.newBuilder().setResource(this.source).build())
                .setPlan(TaskPlan.newBuilder().addAllPlan(this.plan.getPlan()))
                .setSink(sink)
                .build();
    }
}
