/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.graphscope.example.giraph.circle;

import com.google.common.collect.Lists;

import org.apache.giraph.Algorithm;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

@Algorithm(name = "Circle", description = "Finds Circle")
public class Circle
        extends BasicComputation<
                LongWritable, VertexAttrWritable, LongWritable, VertexAttrWritable> {
    private static final Logger logger = LoggerFactory.getLogger(Circle.class);
    int maxIteration = 3;

    public Circle() {}

    public void preSuperstep() {
        this.maxIteration = Integer.parseInt(this.getConf().get("max", "3"));
        logger.info("[preSuperstep] max is {}", this.maxIteration);
    }

    public void compute(
            Vertex<LongWritable, VertexAttrWritable, LongWritable> vertex,
            Iterable<VertexAttrWritable> messages)
            throws IOException {
        this.maxIteration = Integer.parseInt(this.getConf().get("max", "3"));
        long superStep = this.getSuperstep();

        List<MsgWritable> writables = new ArrayList<MsgWritable>();
        for (int i = 0; i < vertex.getId().get() % 5; ++i) {
            writables.add(new MsgWritable(Arrays.asList(1L, 2L, 3L), Arrays.asList(12L, 23L)));
        }
        vertex.setValue(new VertexAttrWritable(writables));
        vertex.voteToHalt();
    }

    private VertexAttrWritable mergeMsg(Iterable<VertexAttrWritable> messages) {
        VertexAttrWritable merged = new VertexAttrWritable();

        VertexAttrWritable mess;
        for (Iterator var3 = messages.iterator();
                var3.hasNext();
                merged = this.merge(merged, mess)) {
            mess = (VertexAttrWritable) var3.next();
        }

        return merged;
    }

    private void vprog(long vid, VertexAttrWritable vdata, VertexAttrWritable message) {
        long superStep = this.getSuperstep();
        List<MsgWritable> nodeAttr = vdata.getVertexAttr();
        List<MsgWritable> messageAttr = message.getVertexAttr();
        List<MsgWritable> processedMsg =
                (List)
                        messageAttr.stream()
                                .peek(
                                        (item) -> {
                                            List<Long> vlist = item.getVertexPath();
                                            this.addVertexToPath(vid, vlist);
                                        })
                                .collect(Collectors.toList());
        nodeAttr.addAll(processedMsg);
        List<MsgWritable> finalNodeAttr =
                (List)
                        nodeAttr.stream()
                                .distinct()
                                .filter(
                                        (item) -> {
                                            List<Long> vertexPath = item.getVertexPath();
                                            return this.filterPathInVertexAttr(
                                                    vertexPath, superStep + 1L);
                                        })
                                .collect(Collectors.toList());
        vdata.setVertexAttr(finalNodeAttr);
    }

    private void addVertexToPath(long vid, List<Long> path) {
        if (path.isEmpty()) {
            path.add(vid);
        } else {
            int pathSize = path.size();
            if (pathSize == 1 && !path.contains(vid)) {
                path.add(vid);
            } else {
                if (!path.subList(1, pathSize).contains(vid)) {
                    path.add(vid);
                }
            }
        }
    }

    private boolean filterPathInVertexAttr(List<Long> vertexPath, long iteration) {
        int pathSize = vertexPath.size();
        return MsgWritable.isCircle(vertexPath) || (long) pathSize == iteration;
    }

    private VertexAttrWritable merge(VertexAttrWritable attr1, VertexAttrWritable attr2) {
        List<MsgWritable> sp1 = attr1.getVertexAttr();
        sp1.addAll(attr2.getVertexAttr());
        return new VertexAttrWritable((List) sp1.stream().distinct().collect(Collectors.toList()));
    }

    private void sendMsg(Vertex<LongWritable, VertexAttrWritable, LongWritable> vertex) {
        List<MsgWritable> currVertexAttr = ((VertexAttrWritable) vertex.getValue()).getVertexAttr();
        long superStep = this.getSuperstep();
        if (!currVertexAttr.isEmpty()) {
            Iterator var5 = vertex.getEdges().iterator();

            while (var5.hasNext()) {
                Edge<LongWritable, LongWritable> edge = (Edge) var5.next();
                long edgeId = ((LongWritable) edge.getValue()).get();
                List<MsgWritable> finalMsgs =
                        (List)
                                currVertexAttr.stream()
                                        .filter(
                                                (path) -> {
                                                    return this.meetMsgCondition(
                                                            ((LongWritable) vertex.getId()).get(),
                                                            edgeId,
                                                            path,
                                                            superStep);
                                                })
                                        .map(
                                                (path) -> {
                                                    List<Long> newEdgeSet =
                                                            Lists.newArrayList(path.getEdgePath());
                                                    newEdgeSet.add(edgeId);
                                                    return new MsgWritable(
                                                            path.getVertexPath(), newEdgeSet);
                                                })
                                        .collect(Collectors.toList());
                if (!finalMsgs.isEmpty()) {
                    this.sendMessage(edge.getTargetVertexId(), new VertexAttrWritable(finalMsgs));
                }
            }
        }
    }

    private boolean meetMsgCondition(
            long currVertexId,
            long edgeIdToSendMsg,
            MsgWritable onePathInCurrVertex,
            long superStep) {
        List<Long> vertexPath = onePathInCurrVertex.getVertexPath();
        List<Long> edgePath = onePathInCurrVertex.getEdgePath();
        int vertexPathSize = vertexPath.size();
        return (long) vertexPathSize == superStep + 1L
                && !MsgWritable.isCircle(vertexPath)
                && currVertexId == (Long) vertexPath.get(vertexPathSize - 1)
                && !edgePath.contains(edgeIdToSendMsg);
    }
}
