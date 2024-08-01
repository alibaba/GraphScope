/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.common.ir.rel.type;

import org.apache.calcite.plan.RelOptTable;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

/**
 * Define the optTable graph which the modification is operated on
 */
public abstract class TargetGraph {
    protected final RelOptTable optTable;
    protected final FieldMappings mappings;
    protected final @Nullable String aliasName;

    public TargetGraph(RelOptTable optTable, FieldMappings mappings, @Nullable String aliasName) {
        this.optTable = Objects.requireNonNull(optTable);
        this.mappings = Objects.requireNonNull(mappings);
        this.aliasName = aliasName;
    }

    public FieldMappings getMappings() {
        return mappings;
    }

    public RelOptTable getOptTable() {
        return optTable;
    }

    public @Nullable String getAliasName() {
        return aliasName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TargetGraph that = (TargetGraph) o;
        return com.google.common.base.Objects.equal(optTable, that.optTable)
                && com.google.common.base.Objects.equal(mappings, that.mappings)
                && com.google.common.base.Objects.equal(aliasName, that.aliasName);
    }

    @Override
    public int hashCode() {
        return com.google.common.base.Objects.hashCode(optTable, mappings, aliasName);
    }

    public static class Vertex extends TargetGraph {
        public Vertex(RelOptTable optTable, FieldMappings mappings, @Nullable String aliasName) {
            super(optTable, mappings, aliasName);
        }

        @Override
        public String toString() {
            return "Vertex{"
                    + "table="
                    + optTable.getQualifiedName()
                    + ", mappings="
                    + mappings
                    + '}';
        }
    }

    public static class Edge extends TargetGraph {
        private TargetGraph srcVertex = null;
        private TargetGraph dstVertex = null;

        public Edge(RelOptTable optTable, FieldMappings mappings, String aliasName) {
            super(optTable, mappings, aliasName);
        }

        public Edge withSrcVertex(TargetGraph srcVertex) {
            this.srcVertex = Objects.requireNonNull(srcVertex);
            return this;
        }

        public Edge withDstVertex(TargetGraph dstVertex) {
            this.dstVertex = Objects.requireNonNull(dstVertex);
            return this;
        }

        public TargetGraph getSrcVertex() {
            return srcVertex;
        }

        public TargetGraph getDstVertex() {
            return dstVertex;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Edge edge = (Edge) o;
            return com.google.common.base.Objects.equal(srcVertex, edge.srcVertex)
                    && com.google.common.base.Objects.equal(dstVertex, edge.dstVertex);
        }

        @Override
        public int hashCode() {
            return com.google.common.base.Objects.hashCode(super.hashCode(), srcVertex, dstVertex);
        }

        @Override
        public String toString() {
            return "Edge{"
                    + "table="
                    + optTable.getQualifiedName()
                    + ", mappings="
                    + mappings
                    + ", srcVertex="
                    + srcVertex
                    + ", dstVertex="
                    + dstVertex
                    + '}';
        }
    }
}
