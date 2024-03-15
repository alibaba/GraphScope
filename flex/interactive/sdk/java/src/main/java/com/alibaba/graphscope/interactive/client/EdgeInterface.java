package com.alibaba.graphscope.interactive.client;

import com.alibaba.graphscope.interactive.client.common.Result;
import org.openapitools.client.model.EdgeData;
import org.openapitools.client.model.EdgeRequest;

/**
 * Interface for Create/Read/Update/Delete edge.
 */
public interface EdgeInterface {
    Result<EdgeData> getEdge(String graphName, String edgeLabel, String srcLabel, Object srcPrimaryKeyValue, String dstLabel, Object dstPrimaryKeyValue);

    Result<String> addEdge(String graphName, EdgeRequest edgeRequest);

    Result<String> deleteEdge(String graphName, String srcLabel, Object srcPrimaryKeyValue, String dstLabel, Object dstPrimaryKeyValue);

    Result<String> updateEdge(String graphName, EdgeRequest edgeRequest);
}
