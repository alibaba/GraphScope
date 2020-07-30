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
package com.alibaba.maxgraph.compiler.executor;

import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.rpc.TimelyResultProcessor;
import com.alibaba.maxgraph.compiler.query.TimelyQuery;

/**
 * Defines methods to execute queries that are described by an Query object.
 * This interface is intended for implementation by query executors.
 */
public interface QueryExecutor<V> {

    /**
     * Executes the query represented by a specified expression tree.
     *
     * @param timelyQuery The timely query
     */
    void execute(TimelyQuery timelyQuery, GraphSchema schema, long timeout, String queryId);

    /**
     * Executes the query represented by a specified expression tree.
     *
     * @param timelyQuery An expression tree.
     */
    void execute(TimelyQuery timelyQuery, ExecuteConfig executeConfig, GraphSchema schema, long timeout, String queryId);

    /**
     * PREPARE the query
     *
     * @param prepareId     The prepare query id
     * @param timelyQuery   The given expression tree
     * @param executeConfig The config of execute
     * @return The instance of PrepareStoreEntity
     */
    void prepare(String prepareId, TimelyQuery timelyQuery, ExecuteConfig executeConfig);

    /**
     * @param prepareId
     * @param timelyQuery
     * @param schema
     */
    void executePrepare(String prepareId, TimelyQuery timelyQuery, GraphSchema schema, String queryId);

    /**
     * Query current process list.
     */
    void showProcessList(TimelyResultProcessor resultProcessor);

    /**
     * Cancel a running dataflow.
     */
    void cancelDataflow(TimelyResultProcessor resultProcessor, String queryId);
}
