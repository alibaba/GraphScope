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

package com.alibaba.graphscope.common.ir.type;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.sql.type.AbstractSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Maintain label for each Entity or Relation: Entity(label), Relation(Label, srcLabel, dstLabel).
 */
public class GraphLabelType extends AbstractSqlType {
    private final List<Entry> labels;

    public GraphLabelType(Entry label) {
        this(ImmutableList.of(label));
    }

    public GraphLabelType(List<Entry> labels) {
        this(labels, SqlTypeName.CHAR);
    }

    public GraphLabelType(Entry label, SqlTypeName typeName) {
        this(ImmutableList.of(label), typeName);
    }

    public GraphLabelType(List<Entry> labels, SqlTypeName typeName) {
        super(typeName, false, null);
        this.labels = ObjectUtils.requireNonEmpty(labels);
        this.computeDigest();
    }

    public Entry getSingleLabelEntry() {
        return labels.get(0);
    }

    public List<Entry> getLabelsEntry() {
        return Collections.unmodifiableList(labels);
    }

    public List<String> getLabelsString() {
        return getLabelsEntry().stream()
                .map(k -> k.toString())
                .collect(Collectors.toUnmodifiableList());
    }

    @Override
    protected void generateTypeString(StringBuilder stringBuilder, boolean b) {
        stringBuilder.append(getLabelsString());
    }

    public void removeLabels(List<Entry> labelsToRemove) {
        this.labels.removeAll(labelsToRemove);
    }

    public static class Entry {
        private String label;
        private Integer labelId;
        @Nullable private String srcLabel;
        @Nullable private Integer srcLabelId;
        @Nullable private String dstLabel;
        @Nullable private Integer dstLabelId;

        public Entry() {
            this.label = StringUtils.EMPTY;
            this.labelId = -1;
        }

        public Entry label(String label) {
            Objects.requireNonNull(label);
            this.label = label;
            return this;
        }

        public Entry labelId(int labelId) {
            this.labelId = labelId;
            return this;
        }

        public Entry srcLabel(String srcLabel) {
            this.srcLabel = srcLabel;
            return this;
        }

        public Entry srcLabelId(int srcLabelId) {
            this.srcLabelId = srcLabelId;
            return this;
        }

        public Entry dstLabel(String dstLabel) {
            this.dstLabel = dstLabel;
            return this;
        }

        public Entry dstLabelId(int dstLabelId) {
            this.dstLabelId = dstLabelId;
            return this;
        }

        public String getLabel() {
            return label;
        }

        public Integer getLabelId() {
            return labelId;
        }

        public @Nullable String getSrcLabel() {
            return srcLabel;
        }

        public @Nullable Integer getSrcLabelId() {
            return srcLabelId;
        }

        public @Nullable String getDstLabel() {
            return dstLabel;
        }

        public @Nullable Integer getDstLabelId() {
            return dstLabelId;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            if (srcLabel == null || dstLabel == null) {
                builder.append("VertexLabel(");
                builder.append(label);
                builder.append(")");
            } else {
                builder.append("EdgeLabel(");
                builder.append(label + ", " + srcLabel + ", " + dstLabel);
                builder.append(")");
            }
            return builder.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Entry entry = (Entry) o;
            return Objects.equals(label, entry.label)
                    && Objects.equals(labelId, entry.labelId)
                    && Objects.equals(srcLabel, entry.srcLabel)
                    && Objects.equals(srcLabelId, entry.srcLabelId)
                    && Objects.equals(dstLabel, entry.dstLabel)
                    && Objects.equals(dstLabelId, entry.dstLabelId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(label, labelId, srcLabel, srcLabelId, dstLabel, dstLabelId);
        }
    }
}
