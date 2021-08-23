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
import com.alibaba.pegasus.service.protocol.PegasusClient.Sink;
import com.alibaba.pegasus.service.protocol.PegasusClient.OrderBy;
import com.alibaba.pegasus.service.protocol.PegasusClient.Union;
import com.alibaba.pegasus.service.protocol.PegasusClient.Subtask;
import com.alibaba.pegasus.service.protocol.PegasusClient.LeftJoin;
import com.alibaba.pegasus.service.protocol.PegasusClient.TaskPlan;
import com.alibaba.pegasus.service.protocol.PegasusClient.Iteration;
import com.alibaba.pegasus.service.protocol.PegasusClient.OperatorDef;
import com.alibaba.pegasus.service.protocol.PegasusClient.OperatorDef.OpKindCase;
import com.alibaba.pegasus.service.protocol.PegasusClient.ChannelDef.ChKindCase;
import com.alibaba.pegasus.service.protocol.PegasusClient.ChannelDef;
import com.alibaba.pegasus.service.protocol.PegasusClient.Exchange;
import com.alibaba.pegasus.service.protocol.PegasusClient.Broadcast;
import com.alibaba.pegasus.service.protocol.PegasusClient.Aggregate;
import com.alibaba.pegasus.service.protocol.PegasusClient.Pipeline;
import com.alibaba.pegasus.service.protocol.PegasusClient.Map;
import com.alibaba.pegasus.service.protocol.PegasusClient.FlatMap;
import com.alibaba.pegasus.service.protocol.PegasusClient.Filter;
import com.alibaba.pegasus.service.protocol.PegasusClient.Range;
import com.alibaba.pegasus.service.protocol.PegasusClient.Limit;
import com.alibaba.pegasus.service.protocol.PegasusClient.Shuffle;
import com.alibaba.pegasus.service.protocol.PegasusClient.Fold;
import com.alibaba.pegasus.service.protocol.PegasusClient.OperatorDef.Builder;
import com.alibaba.pegasus.service.protocol.PegasusClient.AccumKind;
import com.alibaba.pegasus.service.protocol.PegasusClient.GroupBy;
import com.alibaba.pegasus.service.protocol.PegasusClient.Dedup;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Plan {
    private static final Logger logger = LoggerFactory.getLogger(Plan.class);

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
                .setCh(channelDef)
                .setShuffle(Shuffle.newBuilder().build())
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
                .setCh(channelDef)
                .setShuffle(Shuffle.newBuilder().build())
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
                .setCh(channelDef)
                .setShuffle(Shuffle.newBuilder().build())
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
                .setCh(channelDef)
                .setShuffle(Shuffle.newBuilder().build())
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
        Map map = Map.newBuilder()
                .setResource(func).build();
        Builder builder = OperatorDef
                .newBuilder()
                .setCh(channelDef)
                .setMap(map);
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
        FlatMap flatMap = FlatMap
                .newBuilder()
                .setResource(func)
                .build();
        Builder builder = OperatorDef
                .newBuilder()
                .setCh(channelDef)
                .setFlatMap(flatMap);
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
        Filter filter = Filter
                .newBuilder()
                .setResource(func)
                .build();
        Builder builder = OperatorDef
                .newBuilder()
                .setCh(channelDef)
                .setFilter(filter);
        tryChain(builder);
        OperatorDef operatorDef = builder.build();
        this.plan.add(operatorDef);
    }

    public void limit(boolean isGlobal, int n) {
        Limit limitInfo = Limit
                .newBuilder()
                .setRange(isGlobal?Range.GLOBAL:Range.LOCAL)
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
                .setCh(channelDef)
                .setLimit(limitInfo)
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
        Fold fold = Fold.newBuilder()
                .setRange(isGlobal?Range.GLOBAL:Range.LOCAL)
                .setAccum(AccumKind.CNT)
                .build();
        OperatorDef operatorDef = OperatorDef
                .newBuilder()
                .setCh(channelDef)
                .setFold(fold)
                .build();
        this.plan.add(operatorDef);
    }

    public void fold(boolean isGlobal, AccumKind accumKind) {
        Pipeline pipeline = Pipeline
                .newBuilder()
                .build();
        ChannelDef channelDef = ChannelDef
                .newBuilder()
                .setToLocal(pipeline)
                .build();
        Fold fold = Fold.newBuilder()
                .setRange(isGlobal?Range.GLOBAL:Range.LOCAL)
                .setAccum(accumKind)
                .build();
        OperatorDef operatorDef = OperatorDef
                .newBuilder()
                .setCh(channelDef)
                .setFold(fold)
                .build();
        this.plan.add(operatorDef);
    }

    public void foldCustom(boolean isGlobal, AccumKind accumKind, ByteString accumFunc) {
        Pipeline pipeline = Pipeline
                .newBuilder()
                .build();
        ChannelDef channelDef = ChannelDef
                .newBuilder()
                .setToLocal(pipeline)
                .build();
        Fold fold = Fold.newBuilder()
                .setRange(isGlobal?Range.GLOBAL:Range.LOCAL)
                .setAccum(accumKind)
                .setResource(accumFunc)
                .build();
        OperatorDef operatorDef = OperatorDef
                .newBuilder()
                .setCh(channelDef)
                .setFold(fold)
                .build();
        this.plan.add(operatorDef);
    }

    public void dedup(boolean isGlobal, ByteString set) {
        Dedup dedup = Dedup
                .newBuilder()
                .setRange(isGlobal?Range.GLOBAL:Range.LOCAL)
                .setSet(set)
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
                .setCh(channelDef)
                .setDedup(dedup)
                .build();
        this.plan.add(operatorDef);
    }

    public void repeat(int times, Plan subPlan) {
        TaskPlan taskPlan = TaskPlan
                .newBuilder()
                .addAllPlan(subPlan.getPlan())
                .build();
        Iteration iteration  = Iteration
                .newBuilder()
                .setMaxIters(times)
                .setBody(taskPlan)
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
                .setCh(channelDef)
                .setIterate(iteration)
                .build();
        this.plan.add(operatorDef);
    }

    public void repeat(int times, NestedFunc func) {
        Plan repeatedPlan = new Plan();
        func.nestedFunc(repeatedPlan);
        TaskPlan taskPlan = TaskPlan
                .newBuilder()
                .addAllPlan(repeatedPlan.getPlan())
                .build();
        Iteration iteration  = Iteration
                .newBuilder()
                .setMaxIters(times)
                .setBody(taskPlan)
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
                .setCh(channelDef)
                .setIterate(iteration)
                .build();
        this.plan.add(operatorDef);
    }

    public void repeateUntil(int times, ByteString until, Plan subPlan) {
        TaskPlan taskPlan = TaskPlan
                .newBuilder()
                .addAllPlan(subPlan.getPlan())
                .build();
        Filter filterUntil = Filter.newBuilder()
                .setResource(until)
                .build();
        Iteration iteration  = Iteration
                .newBuilder()
                .setMaxIters(times)
                .setBody(taskPlan)
                .setUntil(filterUntil)
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
                .setCh(channelDef)
                .setIterate(iteration)
                .build();
        this.plan.add(operatorDef);
    }

    public void repeateUntil(int times, ByteString until, NestedFunc func) {
        Plan repeatedPlan = new Plan();
        func.nestedFunc(repeatedPlan);
        TaskPlan taskPlan = TaskPlan
                .newBuilder()
                .addAllPlan(repeatedPlan.getPlan())
                .build();
        Filter filterUntil = Filter.newBuilder()
                .setResource(until)
                .build();
        Iteration iteration  = Iteration
                .newBuilder()
                .setMaxIters(times)
                .setBody(taskPlan)
                .setUntil(filterUntil)
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
                .setCh(channelDef)
                .setIterate(iteration)
                .build();
        this.plan.add(operatorDef);
    }

    public void fork(Plan subPlan) {
        TaskPlan taskPlan = TaskPlan
                .newBuilder()
                .addAllPlan(subPlan.getPlan())
                .build();
        Subtask subtask = Subtask
                .newBuilder()
                .setTask(taskPlan)
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
                .setCh(channelDef)
                .setSubtask(subtask)
                .build();
        this.plan.add(operatorDef);
    }

    public void fork(NestedFunc func) {
        Plan subPlan = new Plan();
        func.nestedFunc(subPlan);
        TaskPlan taskPlan = TaskPlan
                .newBuilder()
                .addAllPlan(subPlan.getPlan())
                .build();
        Subtask subtask = Subtask
                .newBuilder()
                .setTask(taskPlan)
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
                .setCh(channelDef)
                .setSubtask(subtask)
                .build();
        this.plan.add(operatorDef);
    }

    public void forkJoin(ByteString joiner, Plan subPlan) {
        TaskPlan taskPlan = TaskPlan
                .newBuilder()
                .addAllPlan(subPlan.getPlan())
                .build();
        LeftJoin leftJoin = LeftJoin
                .newBuilder()
                .setResource(joiner)
                .build();
        Subtask subtask = Subtask
                .newBuilder()
                .setTask(taskPlan)
                .setJoin(leftJoin)
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
                .setCh(channelDef)
                .setSubtask(subtask)
                .build();
        this.plan.add(operatorDef);
    }

    public void forkJoin(ByteString joiner, NestedFunc func) {
        Plan subPlan = new Plan();
        func.nestedFunc(subPlan);
        TaskPlan taskPlan = TaskPlan
                .newBuilder()
                .addAllPlan(subPlan.getPlan())
                .build();
        LeftJoin leftJoin = LeftJoin
                .newBuilder()
                .setResource(joiner)
                .build();
        Subtask subtask = Subtask
                .newBuilder()
                .setTask(taskPlan)
                .setJoin(leftJoin)
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
                .setCh(channelDef)
                .setSubtask(subtask)
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
        List<TaskPlan> unionTasks = new ArrayList<>();
        subPlans.forEach(plan ->
                unionTasks.add(TaskPlan
                        .newBuilder()
                        .addAllPlan(plan.getPlan())
                        .build()));
        Union union = Union
                .newBuilder()
                .addAllBranches(unionTasks)
                .build();
        OperatorDef operatorDef = OperatorDef
                .newBuilder()
                .setCh(channelDef)
                .setUnion(union)
                .build();
        this.plan.add(operatorDef);
    }

    public void sortBy(boolean isGlobal, ByteString cmp) {
        int noLimit = -1;
        OrderBy orderBy = OrderBy
                .newBuilder()
                .setRange(isGlobal?Range.GLOBAL:Range.LOCAL)
                .setLimit(noLimit)
                .setCompare(cmp)
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
                .setCh(channelDef)
                .setOrder(orderBy)
                .build();
        this.plan.add(operatorDef);
    }

    public void topBy(boolean isGlobal, int n, ByteString cmp) {
        OrderBy orderBy = OrderBy
                .newBuilder()
                .setRange(isGlobal?Range.GLOBAL:Range.LOCAL)
                .setLimit(n)
                .setCompare(cmp)
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
                .setCh(channelDef)
                .setOrder(orderBy)
                .build();
        this.plan.add(operatorDef);
    }

    public void groupBy(boolean isGlobal, ByteString map) {
        GroupBy groupBy = GroupBy
                .newBuilder()
                .setRange(isGlobal?Range.GLOBAL:Range.LOCAL)
                .setMap(map)
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
                .setCh(channelDef)
                .setGroup(groupBy)
                .build();
        this.plan.add(operatorDef);
    }

    public Sink sink(ByteString output) {
        return Sink
                .newBuilder()
                .setResource(output)
                .build();
    }

    public void chainUnfold(ByteString func) {
        if (plan.size() > 0) {
            OperatorDef pre = plan.remove(plan.size() - 1);
            OperatorDef.Builder builder = pre.toBuilder();
            if (pre.getOpKindCase() == OpKindCase.GROUP) {
                builder.getGroupBuilder().getUnfoldBuilder().setResource(func);
            } else if (pre.getOpKindCase() == OpKindCase.FOLD) {
                builder.getFoldBuilder().getUnfoldBuilder().setResource(func);
            }
            OperatorDef reduceChain = builder.build();
            plan.add(reduceChain);
        }
    }

    public boolean endReduce() {
        int len = plan.size();
        if (len == 0) {
            return false;
        }
        OpKindCase opKind = plan.get(len - 1).getOpKindCase();
        return opKind == OpKindCase.GROUP || opKind == OpKindCase.FOLD;
    }

    public Sink genSink() {
        Sink.Builder sinkBuilder = Sink.newBuilder();
        if (endReduce()) {
            OperatorDef pre = plan.remove(plan.size() - 1);
            if (pre.getOpKindCase() == OpKindCase.GROUP) {
                sinkBuilder.setGroup(pre.getGroup());
            } else if (pre.getOpKindCase() == OpKindCase.FOLD) {
                sinkBuilder.setFold(pre.getFold());
            }
        }
        return sinkBuilder.build();
    }

    private void tryChain(Builder opBuilder) {
        if (plan.size() > 0) {
            OperatorDef pre = plan.get(plan.size() - 1);
            if (pre.getOpKindCase() != OpKindCase.SHUFFLE) {
                return;
            }
            ChKindCase chKindCase = pre.getCh().getChKindCase();
            if (chKindCase == ChKindCase.TO_ANOTHER
                    || chKindCase == ChKindCase.TO_OTHERS
                    || chKindCase == ChKindCase.TO_ONE) {
                plan.remove(plan.size() - 1);
                opBuilder.setCh(pre.getCh());
            }
        }
    }

    private ByteString toBytes(boolean raw) {
        byte[] bytes = new byte[1];
        bytes[0] = (byte) (raw ? 1 : 0);
        return ByteString.copyFrom(bytes);
    }

}
