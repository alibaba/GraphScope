package com.alibaba.graphscope.interactive.client;

import com.alibaba.graphscope.interactive.client.common.Result;
import org.openapitools.client.model.Graph;
import org.openapitools.client.model.GraphSchema;
import org.openapitools.client.model.JobResponse;
import org.openapitools.client.model.SchemaMapping;

/**
 * All APIs about Graph Creation/Deletion/Updating/Getting, and dataloading.
 */
public interface GraphInterface {
    Result<JobResponse> bulkLoading(String graphId, SchemaMapping mapping);

    Result<String> createGraph(Graph graph);

    Result<String> deleteGraph(String graphId);

    Result<String> updateGraph(String graphId, Graph graph);

    Result<GraphSchema> getGraphSchema(String graphId);

    Result<Graph> getAllGraphs();
}
