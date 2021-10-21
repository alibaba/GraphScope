/**
 * This file is referred and derived from project apache/tinkerpop
 *
 *   https://github.com/apache/tinkerpop/blob/master/gremlin-core/src/main/java/org/apache/tinkerpop/gremlin/process/traversal/dsl/graph/GraphTraversal.java
 *
 * which has the following license:
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.alibaba.maxgraph.tinkerpop.traversal;

import com.alibaba.maxgraph.tinkerpop.steps.ConnectedComponentsStep;
import com.alibaba.maxgraph.tinkerpop.steps.LabelPropagationStep;
import com.alibaba.maxgraph.tinkerpop.steps.OutputVineyardStep;
import com.alibaba.maxgraph.tinkerpop.steps.PageRankStep;
import com.alibaba.maxgraph.tinkerpop.steps.HitsStep;
import com.alibaba.maxgraph.tinkerpop.steps.ShortestPathStep;
import com.alibaba.maxgraph.tinkerpop.steps.AllPathStep;
import com.alibaba.maxgraph.tinkerpop.steps.CustomVertexProgramStep;
import com.alibaba.maxgraph.tinkerpop.steps.EdgeVertexWithByStep;
import com.alibaba.maxgraph.tinkerpop.steps.OutputStep;
import com.alibaba.maxgraph.tinkerpop.steps.VertexByModulatingStep;
import com.alibaba.maxgraph.tinkerpop.steps.VertexWithByStep;
import com.alibaba.maxgraph.tinkerpop.steps.LpaVertexProgramStep;
import com.alibaba.maxgraph.tinkerpop.steps.HitsVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class DefaultMaxGraphTraversal<S, E> extends DefaultGraphTraversal<S, E> {
    public DefaultMaxGraphTraversal() {
        super();
    }

    public DefaultMaxGraphTraversal(final GraphTraversalSource graphTraversalSource) {
        super(graphTraversalSource);
    }

    /**
     * Map the {@link Vertex} to its outgoing incident edges given the edge labels.
     *
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     */
    public GraphTraversal<S, Edge> outE(final String... edgeLabels) {
        this.asAdmin().getBytecode().addStep(GraphTraversal.Symbols.outE, edgeLabels);
        return this.asAdmin().addStep(new VertexByModulatingStep<>(this.asAdmin(), Edge.class, Direction.OUT, edgeLabels));
    }

    public GraphTraversal<S, E> program(final VertexProgram vertexProgram) {
        return this.asAdmin().addStep((Step<E, E>) new CustomVertexProgramStep(this.asAdmin(), vertexProgram));
    }

    public GraphTraversal<S, E> cc(final String outPropId, int iteration) {
        return this.asAdmin().addStep((Step<E, E>) new ConnectedComponentsStep(this.asAdmin(), outPropId, iteration));
    }

    public GraphTraversal<S, E> output(String path, String... properties) {
        return this.asAdmin().addStep((Step<E, E>) new OutputStep(this.asAdmin(), path, properties));
    }

    public GraphTraversal<S, E> outputVineyard(String graphName) {
        return this.asAdmin().addStep((Step<E, E>) new OutputVineyardStep(this.asAdmin(), graphName));
    }

    public GraphTraversal<S, E> lpa(final String direction, final String seedLabel, final String targetLabel, int iteration, final String... edgeLabels) {
        return this.asAdmin().addStep((Step<E, E>) new LabelPropagationStep(this.asAdmin(), direction, seedLabel, targetLabel, iteration, edgeLabels));
    }

    public GraphTraversal<S, E> lpa() {
        return this.asAdmin().addStep((Step<E, E>) new LpaVertexProgramStep(this.asAdmin()));
    }

    public GraphTraversal<S, E> pageRank(final String outPropId, final double alpha, int iteration) {
        return this.asAdmin().addStep((Step<E, E>) new PageRankStep(this.asAdmin(), outPropId, alpha, iteration));
    }

    public GraphTraversal<S, E> hits(final String outPropAuthId, final String outPropHubId, int iteration) {
        return this.asAdmin().addStep((Step<E, E>) new HitsStep(this.asAdmin(), outPropAuthId, outPropHubId, iteration));
    }

    public GraphTraversal<S, E> hits() {
        return this.asAdmin().addStep((Step<E, E>) new HitsVertexProgramStep(this.asAdmin()));
    }

    /**
     * single source shortest path
     *
     * @param sid id of the source vertex
     * @param outPropId  output property name, e.g., "path"
     * @param labelPropId the label for output paths, e.g., "name"
     * @param iteration iteration for algorithm
     * @return the traversal with a shortestPath
     *
     **/
    public GraphTraversal<S, E> shortestPath(final Long sid, final String outPropId, final String labelPropId, int iteration) {
        return this.asAdmin().addStep((Step<E, E>) new ShortestPathStep(this.asAdmin(), sid, outPropId, labelPropId, iteration));
    }

    /**
     * single source shortest path on weighted graph
     *
     * @param sid id of the source vertex
     * @param edgeWeightPropId edge weight property name, e.g., "weight"
     * @param outPropId  output property name, e.g., "path"
     * @param labelPropId the label for output paths, e.g., "name"
     * @param iteration iteration for algorithm
     *
     **/
    public GraphTraversal<S, E> shortestPath(final Long sid, final String edgeWeightPropId, final String outPropId, final String labelPropId, int iteration) {
        return this.asAdmin().addStep((Step<E, E>) new ShortestPathStep(this.asAdmin(), sid, edgeWeightPropId, outPropId, labelPropId, iteration));
    }

    /**
     *  s-t shortest path
     *
     * @param sid id of the source vertex
     * @param tid id of the destination vertex
     * @param outPropId output property name, e.g., "path"
     * @param labelPropId the label for output paths, e.g., "name"
     * @param iteration iteration for algorithm
     * @return
     */

    public GraphTraversal<S, E> shortestPath(final Long sid, final Long tid, final String outPropId, final String labelPropId, int iteration) {
        return this.asAdmin().addStep((Step<E, E>) new ShortestPathStep(this.asAdmin(),sid,tid, outPropId, labelPropId, iteration));
    }

    /**
     * all pair shortest path
     *
     * @param outPropId  output property name, e.g., "path"
     * @param labelPropId the label for output paths, e.g., "name"
     * @param iteration iteration for algorithm
     *
     **/

    public GraphTraversal<S, E> shortestPath(final String outPropId, final String labelPropId, int iteration) {
        return this.asAdmin().addStep((Step<E, E>) new ShortestPathStep(this.asAdmin(), outPropId, labelPropId, iteration));
    }

    /**
     * all pair shortest path on weighted graph
     *
     * @param edgeWeightPropId edge weight property name, e.g., "weight"
     * @param outPropId  output property name, e.g., "path"
     * @param labelPropId the label for output paths, e.g., "name"
     * @param iteration iteration for algorithm
     *
     **/
    public GraphTraversal<S, E> shortestPath(final String edgeWeightPropId, final String outPropId, final String labelPropId, int iteration) {
        return this.asAdmin().addStep((Step<E, E>) new ShortestPathStep(this.asAdmin(), edgeWeightPropId, outPropId, labelPropId, iteration));
    }

    /**
     * s-t paths
     **/
    public GraphTraversal<S, E> allPath(final Long sid, final Long tid, final int khop, final String outPropId) {
        return this.asAdmin().addStep((Step<E, E>) new AllPathStep(this.asAdmin(), sid, tid, khop, outPropId));
    }

    /**
     * Map the {@link Vertex} to its outgoing adjacent vertices given the edge labels.
     *
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     */
    @Override
    public GraphTraversal<S, Vertex> out(final String... edgeLabels) {
        this.asAdmin().getBytecode().addStep(GraphTraversal.Symbols.out, edgeLabels);
        return this.asAdmin().addStep(new VertexWithByStep<>(this.asAdmin(), Vertex.class, Direction.OUT, edgeLabels));
    }

    /**
     * Map the {@link Vertex} to its incoming adjacent vertices given the edge labels.
     *
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     */
    @Override
    public GraphTraversal<S, Vertex> in(final String... edgeLabels) {
        this.asAdmin().getBytecode().addStep(GraphTraversal.Symbols.in, edgeLabels);
        return this.asAdmin().addStep(new VertexWithByStep<>(this.asAdmin(), Vertex.class, Direction.IN, edgeLabels));
    }

    /**
     * Map the {@link Vertex} to its adjacent vertices given the edge labels.
     *
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     */
    @Override
    public GraphTraversal<S, Vertex> both(final String... edgeLabels) {
        this.asAdmin().getBytecode().addStep(GraphTraversal.Symbols.both, edgeLabels);
        return this.asAdmin().addStep(new VertexWithByStep<>(this.asAdmin(), Vertex.class, Direction.BOTH, edgeLabels));
    }

    /**
     * Map the {@link Edge} to its outgoing/tail incident {@link Vertex}.
     *
     * @return the traversal with an appended {@link EdgeVertexStep}.
     */
    @Override
    public GraphTraversal<S, Vertex> outV() {
        this.asAdmin().getBytecode().addStep(GraphTraversal.Symbols.outV);
        return this.asAdmin().addStep(new EdgeVertexWithByStep(this.asAdmin(), Direction.OUT));
    }

    /**
     * Map the {@link Edge} to its incoming/head incident {@link Vertex}.
     *
     * @return the traversal with an appended {@link EdgeVertexStep}.
     */
    @Override
    public GraphTraversal<S, Vertex> inV() {
        this.asAdmin().getBytecode().addStep(GraphTraversal.Symbols.inV);
        return this.asAdmin().addStep(new EdgeVertexWithByStep(this.asAdmin(), Direction.IN));
    }
}
