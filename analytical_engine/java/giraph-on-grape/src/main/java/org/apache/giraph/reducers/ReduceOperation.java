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
package org.apache.giraph.reducers;

import org.apache.hadoop.io.Writable;

/**
 * Reduce operations defining how to reduce single values passed on workers, into partial values on
 * workers, and then into a single global reduced value.
 * <p>
 * Object should be thread safe. Most frequently it should be immutable object, so that functions
 * can execute concurrently. Rarely when object is mutable ({@link org.apache.giraph.master.AggregatorReduceOperation}),
 * i.e. stores reusable object inside, accesses should be synchronized.
 *
 * @param <S> Single value type, objects passed on workers
 * @param <R> Reduced value type
 */
public interface ReduceOperation<S, R extends Writable> extends Writable {

    /**
     * Return new reduced value which is neutral to reduce operation.
     *
     * @return Neutral value
     */
    R createInitialValue();

    /**
     * Add a new value. Needs to be commutative and associative
     * <p>
     * Commonly, returned value should be same as curValue argument.
     *
     * @param curValue      Partial value into which to reduce and store the result
     * @param valueToReduce Single value to be reduced
     * @return reduced value
     */
    R reduce(R curValue, S valueToReduce);

    /**
     * Add partially reduced value to current partially reduced value.
     * <p>
     * Commonly, returned value should be same as curValue argument.
     *
     * @param curValue      Partial value into which to reduce and store the result
     * @param valueToReduce Partial value to be reduced
     * @return reduced value
     */
    R reduceMerge(R curValue, R valueToReduce);
}
