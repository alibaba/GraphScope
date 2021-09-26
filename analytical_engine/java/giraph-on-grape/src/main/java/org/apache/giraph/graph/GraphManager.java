package org.apache.giraph.graph;

import java.io.IOException;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.OutEdges;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Define the interface for a GraphManager, which is responsible for adding, removing vertex, edge
 * request.
 */
public interface GraphManager<OID_T extends WritableComparable, VDATA_T extends Writable, EDATA_T extends Writable> {
    void addVertexRequest(OID_T id, VDATA_T value, OutEdges<OID_T, EDATA_T> edges) throws IOException;

    void addVertexRequest(OID_T id, VDATA_T value) throws IOException;

    void removeVertexRequest(OID_T vertexId) throws IOException;

    void addEdgeRequest(OID_T sourceVertexId, Edge<OID_T, EDATA_T> edge) throws IOException;

    void removeEdgesRequest(OID_T sourceVertexId, OID_T targetVertexId)
        throws IOException;
}
