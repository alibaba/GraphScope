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

package com.alibaba.graphscope.common.ir.meta.glogue.calcite.handler;

import com.alibaba.graphscope.common.ir.rel.GraphPattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.GlogueEdge;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.GlogueQuery;
import com.google.common.base.Preconditions;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Set;

public class GraphGlogueEdgesHandler implements ExternalMetaData.GlogueEdges.Handler {
    private final GlogueQuery glogueQuery;

    public GraphGlogueEdgesHandler(GlogueQuery glogueQuery) {
        this.glogueQuery = glogueQuery;
    }

    @Override
    public @Nullable Set<GlogueEdge> getGlogueEdges(RelNode node, RelMetadataQuery mq) {
        Preconditions.checkArgument(
                node instanceof GraphPattern,
                "can not find incoming glogue edges for the node=" + node.getClass());
        return glogueQuery.getInEdges(((GraphPattern) node).getPattern());
    }
}
