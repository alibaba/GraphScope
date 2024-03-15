package com.alibaba.graphscope.interactive.client;

import com.alibaba.graphscope.interactive.client.common.Result;
import org.openapitools.client.model.Procedure;

import java.util.List;

/**
 * All APIs about procedure management.
 * TODO(zhanglei): differ between ProcedureRequest and Procedure
 */
public interface ProcedureInterface {
    Result<String> createProcedure(String graphId, Procedure procedure);

    Result<String> deleteProcedure(String graphId, String procedureName);

    Result<Procedure> getProcedure(String graphId, String procedureName);

    Result<List<Procedure>> listProcedures(String graphId);

    Result<String> updateProcedure(String graphId, String procedureId, Procedure procedure);

    //TODO(zhanglei): Define call procedure.
    Result<String> callProcedure();
}
