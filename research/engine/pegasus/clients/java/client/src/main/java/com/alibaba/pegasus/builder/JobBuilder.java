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
package com.alibaba.pegasus.builder;

import com.alibaba.pegasus.intf.NestedFunc;
import com.alibaba.pegasus.service.protocol.PegasusClient.AccumKind;
import com.alibaba.pegasus.service.protocol.PegasusClient.JobConfig;
import com.alibaba.pegasus.service.protocol.PegasusClient.JobRequest;
import com.alibaba.pegasus.service.protocol.PegasusClient.Sink;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class JobBuilder extends AbstractBuilder {
    private static final Logger logger = LoggerFactory.getLogger(JobBuilder.class);

    public JobBuilder(JobConfig conf, ByteString source, Plan plan, Sink sink) {
        super(conf, source, plan, sink);
    }

    public JobBuilder(JobConfig conf, ByteString source, Plan plan) {
        super(conf, source, plan);
    }

    public JobBuilder(JobConfig conf, ByteString source) {
        super(conf, source);
    }

    public JobBuilder(JobConfig conf) {
        super(conf);
    }

    public JobBuilder() {
        super();
    }

    public JobBuilder addSource(ByteString source) {
        this.source = source;
        return this;
    }

    public JobBuilder exchange(ByteString route) {
        this.plan.exchange(route);
        return this;
    }

    public JobBuilder broadcast() {
        this.plan.broadcast();
        return this;
    }

    public JobBuilder broadcastBy(ByteString route) {
        this.plan.broadcastBy(route);
        return this;
    }

    public JobBuilder aggregate(int target) {
        this.plan.aggregate(target);
        return this;
    }

    public JobBuilder map(ByteString func) {
        this.plan.map(func);
        return this;
    }

    public JobBuilder flatMap(ByteString func) {
        this.plan.flatMap(func);
        return this;
    }

    public JobBuilder filter(ByteString func) {
        this.plan.filter(func);
        return this;
    }

    public JobBuilder limit(int n) {
        this.plan.limit(n);
        return this;
    }

    public JobBuilder dedup() {
        this.plan.dedup();
        return this;
    }

    public JobBuilder repeat(int times, JobBuilder subPlan) {
        this.plan.repeat(times, subPlan.plan);
        return this;
    }

    public JobBuilder repeat(int times, NestedFunc func) {
        this.plan.repeat(times, func);
        return this;
    }

    public JobBuilder repeatUntil(int times, ByteString until, JobBuilder subPlan) {
        this.plan.repeateUntil(times, until, subPlan.plan);
        return this;
    }

    public JobBuilder repeatUntil(int times, ByteString until, NestedFunc func) {
        this.plan.repeateUntil(times, until, func);
        return this;
    }

    public JobBuilder fork(JobBuilder subPlan) {
        this.plan.fork(subPlan.getPlan());
        return this;
    }

    public JobBuilder fork(NestedFunc func) {
        this.plan.fork(func);
        return this;
    }

    public JobBuilder forkJoin(ByteString joiner, JobBuilder subPlan) {
        this.plan.forkJoin(joiner, subPlan.getPlan());
        return this;
    }

    public JobBuilder forkJoin(ByteString joiner, NestedFunc func) {
        this.plan.forkJoin(joiner, func);
        return this;
    }

    public JobBuilder union(List<JobBuilder> subPlans) {
        List<Plan> plans = new ArrayList<>();
        subPlans.forEach(builder -> plans.add(builder.getPlan()));
        this.plan.union(plans);
        return this;
    }

    public JobBuilder sortBy(ByteString cmp) {
        this.plan.sortBy(cmp);
        return this;
    }

    public JobBuilder topBy(int n, ByteString cmp) {
        this.plan.topBy(n, cmp);
        return this;
    }

    // reduce api
    public ReduceBuilder count() {
        this.plan.count();
        return new ReduceBuilder(this.conf, this.source, this.plan, this.sink);
    }

    // reduce api
    public ReduceBuilder fold(AccumKind accumKind) {
        this.plan.fold(accumKind);
        return new ReduceBuilder(this.conf, this.source, this.plan, this.sink);
    }

    // reduce api
    public ReduceBuilder foldCustom(AccumKind accumKind, ByteString accumFunc) {
        this.plan.foldCustom(accumKind, accumFunc);
        return new ReduceBuilder(this.conf, this.source, this.plan, this.sink);
    }

    // reduce api
    public ReduceBuilder groupBy(AccumKind accumKind, ByteString keySelector) {
        this.plan.groupBy(accumKind, keySelector);
        return new ReduceBuilder(this.conf, this.source, this.plan, this.sink);
    }

    public void sink(ByteString output) {
        this.sink = this.plan.sink(output);
    }

    public static void main(String[] args) {
        JobConfig confPb = JobConfig.newBuilder().setJobId(1).setJobName("test").setWorkers(1).build();
        ByteString opBody = ByteString.EMPTY; // should be converted from pb to bytes
        JobBuilder jobBuilder = new JobBuilder(confPb, opBody);
        // for nested task
        JobBuilder plan = new JobBuilder();
        // build JobReq
        JobRequest jobReq = jobBuilder
                .map(opBody)
                .flatMap(opBody)
                .repeat(3, plan
                        .flatMap(opBody)
                        .flatMap(opBody))
                .count()
                .unfold(opBody)
                .count()
                .build();
        logger.info("send job req: {}", jobReq.toString());
    }
}
