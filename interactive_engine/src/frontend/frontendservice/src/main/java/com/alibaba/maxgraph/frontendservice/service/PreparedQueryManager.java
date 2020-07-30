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
package com.alibaba.maxgraph.frontendservice.service;


import com.alibaba.maxgraph.frontendservice.Frontend;
import com.alibaba.maxgraph.frontendservice.exceptions.PrepareException;

import javax.annotation.Nonnull;
import java.util.List;

public interface PreparedQueryManager {


    void init(@Nonnull Frontend frontend) throws Exception;

    /**
     * PREPARE the query plan on the Runtime engine, persist this plan with name to some durable storage
     * if it is prepared successfully.
     *
     * @param queryDesc     query description in binary.
     * @param name          name or signature of the query plan.
     * @param isOverwrite   is overwrite if prepare statement with the name already exist.
     * @throws PrepareException
     */
    void prepare(final byte[] queryDesc, final String name, boolean isOverwrite) throws PrepareException;


    /**
     * List all prepared query plans' names.
     * @return name list of already prepared queries.
     */
    List<String> listPreparedQueryNames() throws Exception;


    /**
     * Remove a prepared query plan by the name.
     * @param name
     * @throws PrepareException
     */
    void remove(@Nonnull final String name) throws PrepareException;
}
