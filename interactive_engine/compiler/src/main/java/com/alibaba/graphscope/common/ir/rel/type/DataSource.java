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

import com.alibaba.graphscope.common.ir.rex.RexGraphDynamicParam;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

/**
 * Define external source to load data from, which adheres to the {@code DataSource} in data loading specification.
 */
public interface DataSource {
    ImmutableList<String> getName();

    DataFormat getFormat();

    class External implements DataSource {
        private final String location;
        private final DataFormat format;

        /**
         * can be {@code RexLiteral} or {@code RexDynamicParams}, which value is a file name
         */
        private final RexNode input;

        public External(RexNode input, DataFormat format) {
            this("", input, format);
        }

        public External(String location, RexNode input, DataFormat format) {
            this.location = Objects.requireNonNull(location);
            this.input = Objects.requireNonNull(input);
            this.format = Objects.requireNonNull(format);
        }

        public String getLocation() {
            return location;
        }

        public RexNode getInput() {
            return input;
        }

        @Override
        public ImmutableList<String> getName() {
            String inputName = getName(input);
            return ImmutableList.of(
                    location.isEmpty() ? inputName : String.format("%s:%s", location, inputName));
        }

        @Override
        public DataFormat getFormat() {
            return this.format;
        }

        private @Nullable String getName(RexNode rex) {
            if (rex instanceof RexLiteral) {
                RexLiteral literal = (RexLiteral) rex;
                if (literal.getType().getSqlTypeName() == SqlTypeName.CHAR) {
                    return literal.getValueAs(NlsString.class).getValue();
                }
            }
            if (rex instanceof RexGraphDynamicParam) {
                return ((RexGraphDynamicParam) rex).getName();
            }
            throw new IllegalArgumentException("cannot derive table name from the rex=" + rex);
        }
    }

    class Stream implements DataSource {
        @Override
        public ImmutableList<String> getName() {
            return ImmutableList.of("stream");
        }

        @Override
        public DataFormat getFormat() {
            return new DataFormat();
        }
    }
}
