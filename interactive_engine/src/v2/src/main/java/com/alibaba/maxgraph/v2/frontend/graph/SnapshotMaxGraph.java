/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.v2.frontend.graph;

import com.alibaba.maxgraph.v2.common.frontend.api.exception.GraphWriteDataException;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.MaxGraphReader;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.MaxGraphWriter;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.structure.ElementId;
import com.alibaba.maxgraph.v2.common.frontend.api.manager.AlterEdgeTypeManager;
import com.alibaba.maxgraph.v2.common.frontend.api.manager.AlterVertexTypeManager;
import com.alibaba.maxgraph.v2.common.frontend.api.manager.CreateEdgeTypeManager;
import com.alibaba.maxgraph.v2.common.frontend.api.manager.CreateVertexTypeManager;
import com.alibaba.maxgraph.v2.common.frontend.api.manager.DropEdgeTypeManager;
import com.alibaba.maxgraph.v2.common.frontend.api.manager.DropVertexTypeManager;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.EdgeRelation;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.EdgeType;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.SchemaFetcher;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.VertexType;
import com.alibaba.maxgraph.v2.common.schema.GraphSchemaMapper;
import com.alibaba.maxgraph.v2.frontend.graph.schema.MaxGraphAlterEdgeTypeManager;
import com.alibaba.maxgraph.v2.frontend.graph.schema.MaxGraphAlterVertexTypeManager;
import com.alibaba.maxgraph.v2.frontend.graph.schema.MaxGraphCreateEdgeTypeManager;
import com.alibaba.maxgraph.v2.frontend.graph.schema.MaxGraphCreateVertexTypeManager;
import com.alibaba.maxgraph.v2.frontend.graph.schema.MaxGraphDropEdgeTypeManager;
import com.alibaba.maxgraph.v2.frontend.graph.schema.MaxGraphDropVertexTypeManager;
import com.alibaba.maxgraph.v2.frontend.graph.transaction.SnapshotBatchTransaction;
import com.alibaba.maxgraph.v2.frontend.server.gremlin.variables.MaxGraphFeatures;
import com.alibaba.maxgraph.v2.frontend.server.gremlin.variables.MaxGraphVariables;
import com.alibaba.maxgraph.v2.frontend.utils.KeyValueUtil;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.alibaba.maxgraph.v2.frontend.graph.GraphConstants.WRITE_TIMEOUT_MILLSEC;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Snapshot graph of max graph, it will bind to given {@link MaxGraphReader} be used in MaxVertex/MaxEdge/MaxVertexProperty/MaxProperty
 */
public class SnapshotMaxGraph implements Graph {
    private MaxGraphReader reader;
    private MaxGraphWriter writer;
    private SchemaFetcher schemaFetcher;
    private Transaction tx = new SnapshotBatchTransaction(this);

    public void initialize(MaxGraphReader reader, MaxGraphWriter writer, SchemaFetcher schemaFetcher) {
        this.reader = checkNotNull(reader, "reader cant be null");
        this.writer = checkNotNull(writer, "writer cant be null");
        this.schemaFetcher = checkNotNull(schemaFetcher, "schema fetcher cant be null");
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        Map<Object, Object> keyValueMap = KeyValueUtil.convertToMap(keyValues);
        String label = (String) keyValueMap.remove(T.label);
        if (null == label) {
            label = Vertex.DEFAULT_LABEL;
        }
        Map<String, Object> properties = Maps.newHashMap();
        for (Map.Entry<Object, Object> entry : keyValueMap.entrySet()) {
            properties.put(entry.getKey().toString(), entry.getValue());
        }

        ElementId vertexId;
        try {
            vertexId = writer.insertVertex(label, properties).get(WRITE_TIMEOUT_MILLSEC, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new GraphWriteDataException("insert vertex fail", e);
        }
        return reader.getVertex(vertexId);
    }

    public void addVertexAsync(Object... keyValues) {
        Map<Object, Object> keyValueMap = KeyValueUtil.convertToMap(keyValues);
        String label = (String) keyValueMap.remove(T.label);
        if (null == label) {
            label = Vertex.DEFAULT_LABEL;
        }
        Map<String, Object> properties = Maps.newHashMap();
        for (Map.Entry<Object, Object> entry : keyValueMap.entrySet()) {
            properties.put(entry.getKey().toString(), entry.getValue());
        }

        writer.insertVertex(label, properties);
    }

    public void addEdgeAsync(Vertex src, Vertex dst, String label, Object... keyValues) {
        Map<Object, Object> keyValueMap = KeyValueUtil.convertToMap(keyValues);
        Map<String, Object> properties = Maps.newHashMap();
        for (Map.Entry<Object, Object> entry : keyValueMap.entrySet()) {
            properties.put(entry.getKey().toString(), entry.getValue());
        }
        writer.insertEdge((ElementId) src.id(), (ElementId) dst.id(), label, properties);
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        throw new UnsupportedOperationException("Not support compute yet");
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw new UnsupportedOperationException("Not support compute yet");
    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        Set<ElementId> vidList = Sets.newHashSet();
        for (Object id : vertexIds) {
            vidList.add(KeyValueUtil.convertToElementId(id));
        }

        if (vidList.isEmpty()) {
            return this.reader.scanVertices();
        } else {
            return this.reader.getVertices(vidList);
        }
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        Set<ElementId> eidList = Sets.newHashSet();
        for (Object id : edgeIds) {
            eidList.add(KeyValueUtil.convertToElementId(id));
        }
        if (eidList.isEmpty()) {
            return this.reader.scanEdges();
        } else {
            return this.reader.getEdges(eidList);
        }
    }

    @Override
    public Transaction tx() {
        return tx;
    }

    @Override
    public void close() throws Exception {
        this.tx.close();
    }

    @Override
    public Features features() {
        return new MaxGraphFeatures();
    }

    @Override
    public Variables variables() {
        return new MaxGraphVariables();
    }

    @Override
    public Configuration configuration() {
        return new BaseConfiguration();
    }

    /**
     * Create vertex type method
     *
     * <code>
     * graph.createVertexType("person")
     * .addProperty("id", "int")
     * .addProperty("name", "string", "name of person")
     * .addProperty("age", "int", "age of person, default by 0", 0)
     * .primaryKey("id")   // you can user primaryKey("id1", "id2"...) to specify multiple primary keys
     * </code>
     *
     * @param vertexLabel The given vertex label
     * @return The create vertex type manager
     */
    public CreateVertexTypeManager createVertexType(String vertexLabel) {
        return new MaxGraphCreateVertexTypeManager(vertexLabel);
    }

    /**
     * Create edge type method
     * <code>
     * graph.createEdgeType("knows")
     * .addProperty("id", "int")
     * .addProperty("weight", "double", "weight of knows")
     * .addRelation("person", "person")    // add <source vertex --> target vertex> relation
     * </code>
     *
     * @param edgeLabel The given edge label
     * @return The create edge type manager
     */
    public CreateEdgeTypeManager createEdgeType(String edgeLabel) {
        return new MaxGraphCreateEdgeTypeManager(edgeLabel);
    }

    /**
     * Alter vertex type to add/drop property
     * <code>
     * graph.alterVertexType("person")
     * .addProperty("gender", "string")
     * .dropProperty("age")
     * </code>
     *
     * @param vertexType The given vertex label
     * @return The alter vertex type manager
     */
    public AlterVertexTypeManager alterVertexType(String vertexType) {
        return new MaxGraphAlterVertexTypeManager(vertexType);
    }

    /**
     * Alter edge type to add/drop property and add/drop relation
     * <code>
     * graph.alterEdgeType("knows")
     * .addProperty("year", "string")
     * .dropRelation("person", "person")
     * </code>
     *
     * @param edgeType The given edge label
     * @return The alter edge type manager
     */
    public AlterEdgeTypeManager alterEdgeType(String edgeType) {
        return new MaxGraphAlterEdgeTypeManager(edgeType);
    }

    /**
     * Drop vertex with given label
     * <code>
     * graph.dropVertexType("person")
     * </code>
     *
     * @param vertexType The given vertex type
     * @return The drop vertex type manager
     */
    public DropVertexTypeManager dropVertexType(String vertexType) {
        return new MaxGraphDropVertexTypeManager(vertexType);
    }

    /**
     * Drop edge with given label
     * <code>
     * graph.dropEdgeType("knows")
     * </code>
     *
     * @param edgeType The given edge type
     * @return The drop edge type manager
     */
    public DropEdgeTypeManager dropEdgeType(String edgeType) {
        return new MaxGraphDropEdgeTypeManager(edgeType);
    }

    public MaxGraphWriter getGraphWriter() {
        return this.writer;
    }

    public MaxGraphReader getGraphReader() {
        return this.reader;
    }

    /**
     * Generate a reusable {@link GraphTraversalSource} instance.
     * The {@link GraphTraversalSource} provides methods for creating
     * {@link org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal} instances.
     *
     * @return A graph traversal source
     */
    public GraphTraversalSource traversal() {
        return new MaxGraphTraversalSource(this);
    }

    /**
     * Process show process list command
     *
     * @return The show process list query request
     */
    public Object showProcessList() {
        return new ShowProcessListQuery();
    }

    /**
     * Cancel query with given query id
     *
     * @param queryId The given query id
     * @return The cancel query request
     */
    public Object cancel(String queryId) {
        return new CancelDataflow(queryId);
    }

    /**
     * Get schema json from graph
     *
     * @return The schema json
     */
    public String getSchema() {
        return GraphSchemaMapper.parseFromSchema(this.schemaFetcher.fetchSchema().getSchema()).toJsonString();
    }

    /**
     * Load json schema from string
     *
     * @param schemaJsonBytes The given json schema bytes
     * @return The result json schema
     */
    public String loadJsonSchema(String schemaJsonBytes) {
        GraphSchema graphSchema;
        try {
            graphSchema = GraphSchemaMapper.parseFromJson(new String(Hex.decodeHex(schemaJsonBytes))).toGraphSchema();
        } catch (DecoderException e) {
            throw new RuntimeException("decode schema json failed", e);
        }
        this.tx.open();
        for (VertexType vertexType : graphSchema.getVertexTypes()) {
            this.writer.createVertexType(vertexType.getLabel(), vertexType.getPropertyList(), vertexType.getPrimaryKeyConstraint().getPrimaryKeyList());
        }
        for (EdgeType edgeType : graphSchema.getEdgeTypes()) {
            this.writer.createEdgeType(edgeType.getLabel(), edgeType.getPropertyList(), edgeType.getRelationList());
        }
        this.tx.commit();
        return getSchema();
    }

    /**
     * Drop all the schema information
     * @return The dropped schema json string
     */
    public String dropSchema() {
        this.tx.open();
        GraphSchema graphSchema = this.schemaFetcher.fetchSchema().getSchema();
        for (EdgeType edgeType : graphSchema.getEdgeTypes()) {
            String edgeLabel = edgeType.getLabel();
            for (EdgeRelation relation : edgeType.getRelationList()) {
                this.writer.dropEdgeRelation(edgeLabel, relation.getSource().getLabel(),
                        relation.getTarget().getLabel());
            }

            this.writer.dropEdgeType(edgeLabel);
        }
        for (VertexType vertexType : graphSchema.getVertexTypes()) {
            this.writer.dropVertexType(vertexType.getLabel());
        }
        this.tx.commit();
        return GraphSchemaMapper.parseFromSchema(graphSchema).toJsonString();
    }
}
