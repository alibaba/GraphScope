/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.graph;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.OutEdges;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;

/**
 * Define the interface for a GraphManager, which is responsible for adding, removing vertex, edge
 * request.
 */
public interface GraphManager<
        OID_T extends WritableComparable, VDATA_T extends Writable, EDATA_T extends Writable> {

    void addVertexRequest(OID_T id, VDATA_T value, OutEdges<OID_T, EDATA_T> edges)
            throws IOException;

    void addVertexRequest(OID_T id, VDATA_T value) throws IOException;

    void removeVertexRequest(OID_T vertexId) throws IOException;

    void addEdgeRequest(OID_T sourceVertexId, Edge<OID_T, EDATA_T> edge) throws IOException;

    void removeEdgesRequest(OID_T sourceVertexId, OID_T targetVertexId) throws IOException;
}
