package com.alibaba.graphscope.interactive.client;

import com.alibaba.graphscope.interactive.client.common.Result;
import org.openapitools.client.model.VertexData;
import org.openapitools.client.model.VertexRequest;

/**
 * Create/Update/Read/Delete vertex
 */
public interface VertexInterface {
    Result<String> addVertex(String graphId, VertexRequest request);

    Result<String> updateVertex(String graphId, VertexRequest request);

    Result<VertexData> getVertex(String graphId, String label, Object primaryKey);

    Result<String> deleteVertex(String graphId, String label, Object primaryKey);
}
