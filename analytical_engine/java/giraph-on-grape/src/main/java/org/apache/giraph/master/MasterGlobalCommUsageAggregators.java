/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.master;

import org.apache.giraph.reducers.ReduceOperation;
import org.apache.hadoop.io.Writable;

/**
 * Master compute can access reduce and broadcast methods through this interface, from masterCompute
 * method.
 */
public interface MasterGlobalCommUsageAggregators {

    /**
     * Register reducer to be reduced in the next worker computation, using given name and
     * operations.
     *
     * @param name     Name of the reducer
     * @param reduceOp Reduce operations
     * @param <S>      Single value type
     * @param <R>      Reduced value type
     */
    <S, R extends Writable> void registerReducer(String name, ReduceOperation<S, R> reduceOp);

    /**
     * Register reducer to be reduced in the next worker computation, using given name and
     * operations, starting globally from globalInitialValue. (globalInitialValue is reduced only
     * once, each worker will still start from neutral initial value)
     *
     * @param name               Name of the reducer
     * @param reduceOp           Reduce operations
     * @param globalInitialValue Global initial value
     * @param <S>                Single value type
     * @param <R>                Reduced value type
     */
    <S, R extends Writable> void registerReducer(
            String name, ReduceOperation<S, R> reduceOp, R globalInitialValue);

    /**
     * Get reduced value from previous worker computation.
     *
     * @param name Name of the reducer
     * @param <R>  Reduced value type
     * @return Reduced value
     */
    <R extends Writable> R getReduced(String name);

    /**
     * Broadcast given value to all workers for next computation.
     *
     * @param name  Name of the broadcast object
     * @param value Value
     */
    void broadcast(String name, Writable value);
}
