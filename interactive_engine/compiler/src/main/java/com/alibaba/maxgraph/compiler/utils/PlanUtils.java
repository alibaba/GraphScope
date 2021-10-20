/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.compiler.utils;

import com.alibaba.maxgraph.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.jgrapht.Graph;

import java.util.LinkedList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class PlanUtils {

    /**
     * Get order vertex list for given plan
     *
     * @param plan The given plan
     * @return The order vertex list
     */
    public static List<LogicalVertex> getOrderVertexList(Graph<LogicalVertex, LogicalEdge> plan) {
        List<LogicalVertex> logicalVertexList = Lists.newArrayList();

        LinkedList<LogicalVertex> vertexQueue = Lists.newLinkedList();
        vertexQueue.addAll(getSourceVertexList(plan));
        while (!vertexQueue.isEmpty()) {
            LogicalVertex currentVertex = vertexQueue.pop();
            logicalVertexList.add(currentVertex);
            List<LogicalVertex> targetVertexList = getTargetVertexList(plan, currentVertex);
            for (LogicalVertex targetVertex : targetVertexList) {
                List<LogicalVertex> sourceVertexList = getSourceVertexList(plan, targetVertex);
                if (logicalVertexList.containsAll(sourceVertexList)) {
                    vertexQueue.push(targetVertex);
                }
            }
        }

        return logicalVertexList;
    }

    /**
     * Get source vertex list for given plan and vertex
     *
     * @param plan The given plan
     * @param vertex The given vertex
     * @return The source vertex list
     */
    private static List<LogicalVertex> getSourceVertexList(
            Graph<LogicalVertex, LogicalEdge> plan, LogicalVertex vertex) {
        List<LogicalVertex> targetVertexList = Lists.newArrayList();
        for (LogicalEdge logicalEdge : plan.incomingEdgesOf(vertex)) {
            targetVertexList.add(plan.getEdgeSource(logicalEdge));
        }
        return targetVertexList;
    }

    /**
     * Get target vertex list for given plan and vertex
     *
     * @param plan The given plan
     * @param vertex The given vertex
     * @return The target vertex list
     */
    public static List<LogicalVertex> getTargetVertexList(
            Graph<LogicalVertex, LogicalEdge> plan, LogicalVertex vertex) {
        List<LogicalVertex> targetVertexList = Lists.newArrayList();
        for (LogicalEdge logicalEdge : plan.outgoingEdgesOf(vertex)) {
            targetVertexList.add(plan.getEdgeTarget(logicalEdge));
        }
        return targetVertexList;
    }

    /**
     * Get the source vertex for given plan
     *
     * @param plan The given plan
     * @return The source vertex
     */
    public static LogicalVertex getSourceVertex(Graph<LogicalVertex, LogicalEdge> plan) {
        List<LogicalVertex> logicalVertexList = Lists.newArrayList();

        for (LogicalVertex logicalVertex : plan.vertexSet()) {
            if (plan.inDegreeOf(logicalVertex) == 0) {
                logicalVertexList.add(logicalVertex);
            }
        }
        if (logicalVertexList.isEmpty()) {
            return null;
        }

        checkArgument(logicalVertexList.size() == 1, "There's more than one source vertex");
        return logicalVertexList.get(0);
    }

    public static List<LogicalVertex> getSourceVertexList(Graph<LogicalVertex, LogicalEdge> plan) {
        List<LogicalVertex> logicalVertexList = Lists.newArrayList();
        for (LogicalVertex logicalVertex : plan.vertexSet()) {
            if (plan.inDegreeOf(logicalVertex) == 0) {
                logicalVertexList.add(logicalVertex);
            }
        }

        return logicalVertexList;
    }

    /**
     * Get the source edge and vertex pair list for given plan and vertex
     *
     * @param plan The given plan
     * @param vertex The given vertex
     * @return The result list
     */
    public static List<Pair<LogicalEdge, LogicalVertex>> getSourceEdgeVertexList(
            Graph<LogicalVertex, LogicalEdge> plan, LogicalVertex vertex) {
        List<Pair<LogicalEdge, LogicalVertex>> logicalVertexList = Lists.newArrayList();

        for (LogicalEdge logicalEdge : plan.incomingEdgesOf(vertex)) {
            logicalVertexList.add(Pair.of(logicalEdge, plan.getEdgeSource(logicalEdge)));
        }

        return logicalVertexList;
    }

    /**
     * Get the output vertex for given plan
     *
     * @param plan The given plan
     * @return The output vertex
     */
    public static LogicalVertex getOutputVertex(Graph<LogicalVertex, LogicalEdge> plan) {
        List<LogicalVertex> logicalVertexList = Lists.newArrayList();

        for (LogicalVertex logicalVertex : plan.vertexSet()) {
            if (plan.outDegreeOf(logicalVertex) == 0) {
                logicalVertexList.add(logicalVertex);
            }
        }
        if (logicalVertexList.isEmpty()) {
            throw new IllegalArgumentException();
        }

        checkArgument(logicalVertexList.size() == 1, "There's more than one output vertex");
        return logicalVertexList.get(0);
    }

    /**
     * Get the output vertex for given plan
     *
     * @param plan The given plan
     * @return The output vertex
     */
    public static List<LogicalVertex> getOutputVertexList(Graph<LogicalVertex, LogicalEdge> plan) {
        List<LogicalVertex> logicalVertexList = Lists.newArrayList();

        for (LogicalVertex logicalVertex : plan.vertexSet()) {
            if (plan.outDegreeOf(logicalVertex) == 0) {
                logicalVertexList.add(logicalVertex);
            }
        }
        return logicalVertexList;
    }

    /**
     * Get the target edge and vertex pair list for given plan and vertex
     *
     * @param plan The given plan
     * @param vertex The given vertex
     * @return The result list
     */
    public static List<Pair<LogicalEdge, LogicalVertex>> getTargetEdgeVertexList(
            Graph<LogicalVertex, LogicalEdge> plan, LogicalVertex vertex) {
        List<Pair<LogicalEdge, LogicalVertex>> logicalVertexList = Lists.newArrayList();

        for (LogicalEdge logicalEdge : plan.outgoingEdgesOf(vertex)) {
            logicalVertexList.add(Pair.of(logicalEdge, plan.getEdgeTarget(logicalEdge)));
        }

        return logicalVertexList;
    }
}
