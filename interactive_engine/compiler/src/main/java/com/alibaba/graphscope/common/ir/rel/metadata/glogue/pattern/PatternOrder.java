/*
 * Copyright 2024 Alibaba Group Holding Limited.
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

package com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern;

/// PatternOrder is used to reorder the Pattern, and assign a new order id to each PatternVertex.
/// A good pattern order can help to reduce the search space of the pattern matching.
public abstract class PatternOrder {
    /// Given a PatternVertex v, return its new order after pattern ordering.
    public abstract Integer getVertexOrder(PatternVertex vertex);
    /// Given a PatternVertex v, return its group id after pattern ordering.
    /// Notice if two vertices have the same group id, they must structurally equivalent.
    public abstract Integer getVertexGroup(PatternVertex vertex);
    /// Given a order id, return the PatternVertex with the order id after pattern ordering.
    public abstract PatternVertex getVertexByOrder(Integer id);
}
