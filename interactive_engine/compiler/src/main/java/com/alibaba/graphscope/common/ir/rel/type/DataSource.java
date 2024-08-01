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

import java.util.Objects;

/**
 * Define external source to load data from, which adheres to the {@code DataSource} in data loading specification.
 */
public interface DataSource {
    ImmutableList<String> getName();

    class Location implements DataSource {
        /**
         * can be {@code RexLiteral} or {@code RexDynamicParams}, which value is a file path
         */
        private final RexNode location;

        public Location(RexNode location) {
            this.location = Objects.requireNonNull(location);
        }

        public RexNode getLocation() {
            return location;
        }

        @Override
        public ImmutableList<String> getName() {
            String name = null;
            if (location instanceof RexLiteral) {
                RexLiteral literal = (RexLiteral) location;
                if (literal.getType().getSqlTypeName() == SqlTypeName.CHAR) {
                    name = literal.getValueAs(NlsString.class).getValue();
                }
            } else if (location instanceof RexGraphDynamicParam) {
                name = ((RexGraphDynamicParam) location).getName();
            }
            name = (name == null) ? location.toString() : name;
            return ImmutableList.of(name);
        }
    }

    // todo: support Data Source from Schema
}
