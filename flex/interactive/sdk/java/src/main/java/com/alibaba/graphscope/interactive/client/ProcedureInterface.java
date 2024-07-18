package com.alibaba.graphscope.interactive.client;

import com.alibaba.graphscope.gaia.proto.GraphAlgebraPhysical;
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.alibaba.graphscope.gaia.proto.StoredProcedure;
import com.alibaba.graphscope.interactive.client.common.Result;
import com.alibaba.graphscope.interactive.models.QueryRequest;

import java.util.concurrent.CompletableFuture;

public interface ProcedureInterface {
    ///////////// Submitting Queries////////////////////
    Result<IrResult.CollectiveResults> callProcedure(String graphId, QueryRequest request);

    CompletableFuture<Result<IrResult.CollectiveResults>> callProcedureAsync(
            String graphId, QueryRequest request);

    Result<IrResult.CollectiveResults> callProcedure(QueryRequest request);

    CompletableFuture<Result<IrResult.CollectiveResults>> callProcedureAsync(QueryRequest request);

    ///////// Call procedure via stored_procedure.proto//////
    Result<IrResult.CollectiveResults> callProcedure(String graphId, StoredProcedure.Query request);

    CompletableFuture<Result<IrResult.CollectiveResults>> callProcedureAsync(
            String graphId, StoredProcedure.Query request);

    Result<IrResult.CollectiveResults> callProcedure(StoredProcedure.Query request);

    CompletableFuture<Result<IrResult.CollectiveResults>> callProcedureAsync(
            StoredProcedure.Query request);

    /////////// Call procedure via raw bytes//////////////

    Result<byte[]> callProcedureRaw(String graphId, byte[] request);

    CompletableFuture<Result<byte[]>> callProcedureRawAsync(String graphId, byte[] request);

    Result<byte[]> callProcedureRaw(byte[] request);

    CompletableFuture<Result<byte[]>> callProcedureRawAsync(byte[] request);

    /////////// Submitting adhoc queries//////////////
    /**
     * Submit a adhoc query, represented via physical plan.
     * @param graphId the identifier of the graph
     * @param physicalPlan physical execution plan.
     * @return the results.
     */
    Result<IrResult.CollectiveResults> runAdhocQuery(
            String graphId, GraphAlgebraPhysical.PhysicalPlan physicalPlan);

    CompletableFuture<Result<IrResult.CollectiveResults>> runAdhocQueryAsync(
            String graphId, GraphAlgebraPhysical.PhysicalPlan physicalPlan);

    /**
     * Submit a adhoc query, represented via physical plan.
     * @param physicalPlan physical execution plan.
     * @return the results.
     */
    Result<IrResult.CollectiveResults> runAdhocQuery(
            GraphAlgebraPhysical.PhysicalPlan physicalPlan);

    CompletableFuture<Result<IrResult.CollectiveResults>> runAdhocQueryAsync(
            GraphAlgebraPhysical.PhysicalPlan physicalPlan);
}
