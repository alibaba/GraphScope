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
import com.alibaba.pegasus.service.proto.PegasusClient.JobConfig;
import com.alibaba.pegasus.service.proto.PegasusClient.JobRequest;
import com.alibaba.pegasus.service.proto.PegasusClient.AccumKind;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class JobBuilder {
    private static final Logger logger = Logger.getLogger(JobBuilder.class.getName());

    private JobConfig conf;
    private ByteString source;
    private Plan plan;

    public JobBuilder(JobConfig conf, ByteString source, Plan plan) {
        this.conf = conf;
        this.source = source;
        this.plan = plan;
    }

    public JobBuilder(JobConfig conf, ByteString source) {
        this.conf = conf;
        this.source = source;
        this.plan = new Plan();
    }

    public JobBuilder(JobConfig conf) {
        this.conf = conf;
        this.plan = new Plan();
    }

    public JobBuilder() {
        this.plan = new Plan();
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

    public void setSource(ByteString source) {
        this.source = source;
    }

    public Plan getPlan() {
        return plan;
    }

    public void setPlan(Plan plan) {
        this.plan = plan;
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

    public JobBuilder limit(boolean isGlobal, int n) {
        this.plan.limit(isGlobal, n);
        return this;
    }

    public JobBuilder count(boolean isGlobal) {
        this.plan.count(isGlobal);
        return this;
    }

    public JobBuilder dedup(boolean isGlobal) {
        this.plan.dedup(isGlobal);
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

    public JobBuilder sortBy(boolean isGlobal, ByteString cmp) {
        this.plan.sortBy(isGlobal, cmp);
        return this;
    }

    public JobBuilder topBy(boolean isGlobal, int n, ByteString cmp) {
        this.plan.topBy(isGlobal, n, cmp);
        return this;
    }

    public JobBuilder groupBy(boolean isGlobal, ByteString getKey, AccumKind accumKind, ByteString accumFunc) {
        this.plan.groupBy(isGlobal, getKey, accumKind, accumFunc);
        return this;
    }

    public JobBuilder sink(ByteString output) {
        this.plan.sink(output);
        return this;
    }

    public JobRequest build() {
        return JobRequest.newBuilder()
                .setConf(this.conf)
                .setSource(this.source)
                .addAllPlan(this.plan.getPlan())
                .build();
    }

    public static void main(String[] args) {
        JobConfig confPb = JobConfig.newBuilder().setJobId(1).setJobName("test").setWorkers(1).build();
        ByteString opBody = ByteString.EMPTY; // should be converted from pb to bytes
        JobBuilder jobBuilder = new JobBuilder(confPb, opBody);
        // for nested task
        JobBuilder plan = new JobBuilder();
        // build JobReq
        JobRequest jobReq = jobBuilder.map(opBody).flatMap(opBody).repeat(3, plan.flatMap(opBody).flatMap(opBody)).build();
        System.out.printf("send job req: %s", jobReq.toString());
    }
}
