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

package com.alibaba.graphscope.common.ir.schema;

import com.alibaba.graphscope.compiler.api.schema.GraphSchema;

import org.apache.calcite.schema.Statistic;

import java.util.List;

/**
 * Extends {@link GraphSchema} to add {@link Statistic}
 */
public interface StatisticSchema extends GraphSchema {
    // get meta for CBO
    Statistic getStatistic(List<String> tableName);

    // if the property name need to be converted to id
    boolean isColumnId();

    // schema json for ir core
    String schemaJson();
}
