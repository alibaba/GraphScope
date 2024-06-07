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

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.GlogueEdge;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Set;

public abstract class ExternalMetaData {

    /** Metadata to find all incoming {@code GlogueEdge} (s) of a specific {@code Pattern} */
    public interface GlogueEdges extends Metadata {
        /** Handler API. */
        @FunctionalInterface
        interface Handler extends MetadataHandler<ExternalMetaData.GlogueEdges> {
            @Nullable Set<GlogueEdge> getGlogueEdges(RelNode node, RelMetadataQuery mq);

            @Override
            default MetadataDef<ExternalMetaData.GlogueEdges> getDef() {
                return null;
            }
        }
    }
}
