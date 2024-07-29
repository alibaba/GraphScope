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
package com.alibaba.graphscope.interactive.client;

import com.alibaba.graphscope.gaia.proto.GraphAlgebraPhysical;
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.alibaba.graphscope.gaia.proto.StoredProcedure;
import com.alibaba.graphscope.interactive.client.common.Result;
import com.alibaba.graphscope.interactive.models.QueryRequest;

import java.util.concurrent.CompletableFuture;

public interface QueryInterface {
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
