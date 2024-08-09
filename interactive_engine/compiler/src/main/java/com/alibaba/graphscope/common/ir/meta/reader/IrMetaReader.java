/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.ir.meta.reader;

import com.alibaba.graphscope.common.ir.meta.GraphId;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.groot.common.schema.api.GraphStatistics;

import java.io.IOException;

/**
 * {@code IrMetaReader} is used to read Ir Meta from a data source (can be a local file or remote web service).
 */
public interface IrMetaReader {
    IrMeta readMeta() throws IOException;

    // get statistics from a graph referenced by graphId
    GraphStatistics readStats(GraphId graphId) throws IOException;

    // a synchronous invocation to check whether statistics functionality is enabled in the backend
    boolean syncStatsEnabled(GraphId graphId) throws IOException;
}
