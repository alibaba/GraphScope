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
import com.alibaba.pegasus.service.proto.PegasusClient.OperatorDef;
import com.alibaba.pegasus.service.proto.PegasusClient.ChannelDef;
import com.alibaba.pegasus.service.proto.PegasusClient.Exchange;
import com.alibaba.pegasus.service.proto.PegasusClient.Broadcast;
import com.alibaba.pegasus.service.proto.PegasusClient.Aggregate;
import com.alibaba.pegasus.service.proto.PegasusClient.Pipeline;
import com.alibaba.pegasus.service.proto.PegasusClient.OpKind;
import com.alibaba.pegasus.service.proto.PegasusClient.NestedTask;
import com.alibaba.pegasus.service.proto.PegasusClient.RepeatCond;
import com.alibaba.pegasus.service.proto.PegasusClient.Joiner;
import com.alibaba.pegasus.service.proto.PegasusClient.Limit;
import com.alibaba.pegasus.service.proto.PegasusClient.SortBy;
import com.alibaba.pegasus.service.proto.PegasusClient.OperatorDef.Builder;
import com.alibaba.pegasus.service.proto.PegasusClient.AccumKind;
import com.alibaba.pegasus.service.proto.PegasusClient.GroupBy;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class Plan {
    private static final Logger logger = Logger.getLogger(Plan.class.getName());

    private List<OperatorDef> plan;

    public Plan(ArrayList<OperatorDef> plan) {
        this.plan = plan;
    }

    public Plan() {
        this.plan = new ArrayList<OperatorDef>();
    }

    public List<OperatorDef> getPlan() {
        return plan;
    }

    public void setPlan(List<OperatorDef> plan) {
        this.plan = plan;
    }

    public void exchange(ByteString route) {
        Exchange exchange = Exchange
                .newBuilder()
                .setResource(route)
                .build();
        ChannelDef channelDef = ChannelDef
                .newBuilder()
                .setToAnother(exchange)
                .build();
        OperatorDef operatorDef = OperatorDef
                .newBuilder()
                .setOpKind(OpKind.EXCHANGE)
                .setCh(channelDef)
                .build();
        this.plan.add(operatorDef);
    }

    public void broadcast() {
        Broadcast broadcast = Broadcast
                .newBuilder()
                .build();
        ChannelDef channelDef = ChannelDef
                .newBuilder()
                .setToOthers(broadcast)
                .build();
        OperatorDef operatorDef = OperatorDef
                .newBuilder()
                .setOpKind(OpKind.BROADCAST)
                .setCh(channelDef)
                .build();
        this.plan.add(operatorDef);
    }

    public void broadcastBy(ByteString route) {
        Broadcast broadcast = Broadcast
                .newBuilder()
                .setResource(route)
                .build();
        ChannelDef channelDef = ChannelDef
                .newBuilder()
                .setToOthers(broadcast)
                .build();
        OperatorDef operatorDef = OperatorDef
                .newBuilder()
                .setOpKind(OpKind.BROADCAST)
                .setCh(channelDef)
                .build();
        this.plan.add(operatorDef);
    }

    public void aggregate(int target) {
        Aggregate aggregate = Aggregate
                .newBuilder()
                .setTarget(target)
                .build();
        ChannelDef channelDef = ChannelDef
                .newBuilder()
                .setToOne(aggregate)
                .build();
        OperatorDef operatorDef = OperatorDef
                .newBuilder()
                .setOpKind(OpKind.AGGREGATE)
                .setCh(channelDef)
                .build();
        this.plan.add(operatorDef);
    }

    public void map(ByteString func) {
        Pipeline pipeline = Pipeline
                .newBuilder()
                .build();
        ChannelDef channelDef = ChannelDef
                .newBuilder()
                .setToLocal(pipeline)
                .build();
        Builder builder = OperatorDef
                .newBuilder()
                .setOpKind(OpKind.MAP)
                .setCh(channelDef)
                .setResource(func);
        tryChain(builder);
        OperatorDef operatorDef = builder.build();
        this.plan.add(operatorDef);
    }

    public void flatMap(ByteString func) {
        Pipeline pipeline = Pipeline
                .newBuilder()
                .build();
        ChannelDef channelDef = ChannelDef
                .newBuilder()
                .setToLocal(pipeline)
                .build();
        Builder builder = OperatorDef
                .newBuilder()
                .setOpKind(OpKind.FLATMAP)
                .setCh(channelDef)
                .setResource(func);
        tryChain(builder);
        OperatorDef operatorDef = builder.build();
        this.plan.add(operatorDef);
    }

    public void filter(ByteString func) {
        Pipeline pipeline = Pipeline
                .newBuilder()
                .build();
        ChannelDef channelDef = ChannelDef
                .newBuilder()
                .setToLocal(pipeline)
                .build();
        Builder builder = OperatorDef
                .newBuilder()
                .setOpKind(OpKind.FILTER)
                .setCh(channelDef)
                .setResource(func);
        tryChain(builder);
        OperatorDef operatorDef = builder.build();
        this.plan.add(operatorDef);
    }

    public void limit(boolean isGlobal, int n) {
        Limit limitInfo = Limit
                .newBuilder()
                .setGlobal(isGlobal)
                .setSize(n)
                .build();
        Pipeline pipeline = Pipeline
                .newBuilder()
                .build();
        ChannelDef channelDef = ChannelDef
                .newBuilder()
                .setToLocal(pipeline)
                .build();
        OperatorDef operatorDef = OperatorDef
                .newBuilder()
                .setOpKind(OpKind.LIMIT)
                .setCh(channelDef)
                .setResource(limitInfo.toByteString())
                .build();
        this.plan.add(operatorDef);
    }

    public void count(boolean isGlobal) {
        Pipeline pipeline = Pipeline
                .newBuilder()
                .build();
        ChannelDef channelDef = ChannelDef
                .newBuilder()
                .setToLocal(pipeline)
                .build();
        OperatorDef operatorDef = OperatorDef
                .newBuilder()
                .setOpKind(OpKind.COUNT)
                .setCh(channelDef)
                .setResource(toBytes(isGlobal))
                .build();
        this.plan.add(operatorDef);
    }

    public void dedup(boolean isGlobal) {
        Pipeline pipeline = Pipeline
                .newBuilder()
                .build();
        ChannelDef channelDef = ChannelDef
                .newBuilder()
                .setToLocal(pipeline)
                .build();
        OperatorDef operatorDef = OperatorDef
                .newBuilder()
                .setOpKind(OpKind.DEDUP)
                .setCh(channelDef)
                .setResource(toBytes(isGlobal))
                .build();
        this.plan.add(operatorDef);
    }

    public void repeat(int times, Plan subPlan) {
        Pipeline pipeline = Pipeline
                .newBuilder()
                .build();
        ChannelDef channelDef = ChannelDef
                .newBuilder()
                .setToLocal(pipeline)
                .build();
        RepeatCond repeatCond = RepeatCond
                .newBuilder()
                .setTimes(times)
                .build();
        NestedTask nestedTask = NestedTask
                .newBuilder()
                .setRepeatCond(repeatCond)
                .addAllPlan(subPlan.getPlan())
                .build();
        OperatorDef operatorDef = OperatorDef
                .newBuilder()
                .setOpKind(OpKind.REPEAT)
                .setCh(channelDef)
                .addNestedTask(nestedTask)
                .build();
        this.plan.add(operatorDef);
    }

    public void repeat(int times, NestedFunc func) {
        Plan repeatedPlan = new Plan();
        func.nestedFunc(repeatedPlan);
        Pipeline pipeline = Pipeline
                .newBuilder()
                .build();
        ChannelDef channelDef = ChannelDef
                .newBuilder()
                .setToLocal(pipeline)
                .build();
        RepeatCond repeatCond = RepeatCond
                .newBuilder()
                .setTimes(times)
                .build();
        NestedTask nestedTask = NestedTask
                .newBuilder()
                .setRepeatCond(repeatCond)
                .addAllPlan(repeatedPlan.getPlan())
                .build();
        OperatorDef operatorDef = OperatorDef
                .newBuilder()
                .setOpKind(OpKind.REPEAT)
                .setCh(channelDef)
                .addNestedTask(nestedTask)
                .build();
        this.plan.add(operatorDef);
    }

    public void repeateUntil(int times, ByteString until, Plan subPlan) {
        Pipeline pipeline = Pipeline
                .newBuilder()
                .build();
        ChannelDef channelDef = ChannelDef
                .newBuilder()
                .setToLocal(pipeline)
                .build();
        RepeatCond repeatCond = RepeatCond
                .newBuilder()
                .setTimes(times)
                .setUntil(until)
                .build();
        NestedTask nestedTask = NestedTask
                .newBuilder()
                .setRepeatCond(repeatCond)
                .addAllPlan(subPlan.getPlan())
                .build();
        OperatorDef operatorDef = OperatorDef
                .newBuilder()
                .setOpKind(OpKind.REPEAT)
                .setCh(channelDef)
                .addNestedTask(nestedTask)
                .build();
        this.plan.add(operatorDef);
    }

    public void repeateUntil(int times, ByteString until, NestedFunc func) {
        Plan repeatedPlan = new Plan();
        func.nestedFunc(repeatedPlan);
        Pipeline pipeline = Pipeline
                .newBuilder()
                .build();
        ChannelDef channelDef = ChannelDef
                .newBuilder()
                .setToLocal(pipeline)
                .build();
        RepeatCond repeatCond = RepeatCond
                .newBuilder()
                .setTimes(times)
                .setUntil(until)
                .build();
        NestedTask nestedTask = NestedTask
                .newBuilder()
                .setRepeatCond(repeatCond)
                .addAllPlan(repeatedPlan.getPlan())
                .build();
        OperatorDef operatorDef = OperatorDef
                .newBuilder()
                .setOpKind(OpKind.REPEAT)
                .setCh(channelDef)
                .addNestedTask(nestedTask)
                .build();
        this.plan.add(operatorDef);
    }

    public void fork(Plan subPlan) {
        Pipeline pipeline = Pipeline
                .newBuilder()
                .build();
        ChannelDef channelDef = ChannelDef
                .newBuilder()
                .setToLocal(pipeline)
                .build();
        NestedTask nestedTask = NestedTask
                .newBuilder()
                .addAllPlan(subPlan.getPlan())
                .build();
        OperatorDef operatorDef = OperatorDef
                .newBuilder()
                .setOpKind(OpKind.SUBTASK)
                .setCh(channelDef)
                .addNestedTask(nestedTask)
                .build();
        this.plan.add(operatorDef);
    }

    public void fork(NestedFunc func) {
        Plan subPlan = new Plan();
        func.nestedFunc(subPlan);
        Pipeline pipeline = Pipeline
                .newBuilder()
                .build();
        ChannelDef channelDef = ChannelDef
                .newBuilder()
                .setToLocal(pipeline)
                .build();
        NestedTask nestedTask = NestedTask
                .newBuilder()
                .addAllPlan(subPlan.getPlan())
                .build();
        OperatorDef operatorDef = OperatorDef
                .newBuilder()
                .setOpKind(OpKind.SUBTASK)
                .setCh(channelDef)
                .addNestedTask(nestedTask)
                .build();
        this.plan.add(operatorDef);
    }

    public void forkJoin(ByteString joiner, Plan subPlan) {
        Pipeline pipeline = Pipeline
                .newBuilder()
                .build();
        ChannelDef channelDef = ChannelDef
                .newBuilder()
                .setToLocal(pipeline)
                .build();
        Joiner joinerPb = Joiner
                .newBuilder()
                .setJoiner(joiner)
                .build();
        NestedTask nestedTask = NestedTask
                .newBuilder()
                .setJoiner(joinerPb)
                .addAllPlan(subPlan.getPlan())
                .build();
        OperatorDef operatorDef = OperatorDef
                .newBuilder()
                .setOpKind(OpKind.SUBTASK)
                .setCh(channelDef)
                .addNestedTask(nestedTask)
                .build();
        this.plan.add(operatorDef);
    }

    public void forkJoin(ByteString joiner, NestedFunc func) {
        Plan subPlan = new Plan();
        func.nestedFunc(subPlan);
        Pipeline pipeline = Pipeline
                .newBuilder()
                .build();
        ChannelDef channelDef = ChannelDef
                .newBuilder()
                .setToLocal(pipeline)
                .build();
        Joiner joinerPb = Joiner
                .newBuilder()
                .setJoiner(joiner)
                .build();
        NestedTask nestedTask = NestedTask
                .newBuilder()
                .setJoiner(joinerPb)
                .addAllPlan(subPlan.getPlan())
                .build();
        OperatorDef operatorDef = OperatorDef
                .newBuilder()
                .setOpKind(OpKind.SUBTASK)
                .setCh(channelDef)
                .addNestedTask(nestedTask)
                .build();
        this.plan.add(operatorDef);
    }

    public void union(List<Plan> subPlans) {
        Pipeline pipeline = Pipeline
                .newBuilder()
                .build();
        ChannelDef channelDef = ChannelDef
                .newBuilder()
                .setToLocal(pipeline)
                .build();
        List<NestedTask> nestedTask = new ArrayList<>();
        subPlans.forEach(plan ->
                nestedTask.add(NestedTask
                        .newBuilder()
                        .addAllPlan(plan.getPlan())
                        .build()));
        OperatorDef operatorDef = OperatorDef
                .newBuilder()
                .setOpKind(OpKind.UNION)
                .setCh(channelDef)
                .addAllNestedTask(nestedTask)
                .build();
        this.plan.add(operatorDef);
    }

    public void sortBy(boolean isGlobal, ByteString cmp) {
        int noLimit = -1;
        SortBy sortBy = SortBy
                .newBuilder()
                .setGlobal(isGlobal)
                .setCmp(cmp)
                .setLimit(noLimit)
                .build();
        Pipeline pipeline = Pipeline
                .newBuilder()
                .build();
        ChannelDef channelDef = ChannelDef
                .newBuilder()
                .setToLocal(pipeline)
                .build();
        OperatorDef operatorDef = OperatorDef
                .newBuilder()
                .setOpKind(OpKind.SORT)
                .setCh(channelDef)
                .setResource(sortBy.toByteString())
                .build();
        this.plan.add(operatorDef);
    }

    public void topBy(boolean isGlobal, int n, ByteString cmp) {
        SortBy sortBy = SortBy
                .newBuilder()
                .setGlobal(isGlobal)
                .setCmp(cmp)
                .setLimit(n)
                .build();
        Pipeline pipeline = Pipeline
                .newBuilder()
                .build();
        ChannelDef channelDef = ChannelDef
                .newBuilder()
                .setToLocal(pipeline)
                .build();
        OperatorDef operatorDef = OperatorDef
                .newBuilder()
                .setOpKind(OpKind.SORT)
                .setCh(channelDef)
                .setResource(sortBy.toByteString())
                .build();
        this.plan.add(operatorDef);
    }

    public void groupBy(boolean isGlobal, ByteString getKey, AccumKind accumKind, ByteString accumFunc) {
        GroupBy groupBy = GroupBy
                .newBuilder()
                .setGlobal(isGlobal)
                .setGetKey(getKey)
                .setAccum(accumKind)
                .setResource(accumFunc)
                .build();
        Pipeline pipeline = Pipeline
                .newBuilder()
                .build();
        ChannelDef channelDef = ChannelDef
                .newBuilder()
                .setToLocal(pipeline)
                .build();
        OperatorDef operatorDef = OperatorDef
                .newBuilder()
                .setOpKind(OpKind.GROUP)
                .setCh(channelDef)
                .setResource(groupBy.toByteString())
                .build();
        this.plan.add(operatorDef);
    }

    public void sink(ByteString output) {
        Pipeline pipeline = Pipeline
                .newBuilder()
                .build();
        ChannelDef channelDef = ChannelDef
                .newBuilder()
                .setToLocal(pipeline)
                .build();
        Builder builder = OperatorDef
                .newBuilder()
                .setOpKind(OpKind.SINK)
                .setCh(channelDef)
                .setResource(output);
        tryChain(builder);
        OperatorDef operatorDef = builder.build();
        this.plan.add(operatorDef);
    }

    private void tryChain(Builder opBuilder) {
        if (plan.size() > 0) {
            OperatorDef pre = plan.remove(plan.size() - 1);
            if (pre.getOpKind() == OpKind.EXCHANGE
                    || pre.getOpKind() == OpKind.BROADCAST
                    || pre.getOpKind() == OpKind.AGGREGATE) {
                opBuilder.setCh(pre.getCh());
            } else {
                plan.add(pre);
            }
        }
    }

    private ByteString toBytes(boolean raw) {
        byte[] bytes = new byte[1];
        bytes[0] = (byte) (raw ? 1 : 0);
        return ByteString.copyFrom(bytes);
    }

}
