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

package com.alibaba.graphscope.common.ir.tools.config;

/**
 * To transform to {@link org.apache.calcite.sql.JoinType} in a {@link org.apache.calcite.rel.core.Join} structure
 */
public enum MatchOpt {
    INNER,
    ANTI, // the sentence is anti, i.e. `not(as("a").out().as("b"))` in gremlin query
    OPTIONAL, // the sentence is optional, still keep the sentence even though it is not joined by
    // any others
}
