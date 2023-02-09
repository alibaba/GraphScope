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

package com.alibaba.graphscope.common.calcite.rel.type.order;

import com.alibaba.graphscope.common.calcite.rex.RexGraphVariable;

import org.apache.calcite.rel.RelFieldCollation;

import java.util.Objects;

/**
 * maintain each pair of order key with direction (asc, desc)
 */
public class GraphFieldCollation extends RelFieldCollation {
    private RexGraphVariable variable;

    public GraphFieldCollation(RexGraphVariable variable, Direction direction) {
        super(-1, direction);
        this.variable = Objects.requireNonNull(variable);
    }

    public GraphFieldCollation(RexGraphVariable variable) {
        this(variable, Direction.ASCENDING);
    }

    public RexGraphVariable getVariable() {
        return this.variable;
    }

    @Override
    public String toString() {
        if (this.direction == RelFieldCollation.Direction.ASCENDING
                && this.nullDirection == this.direction.defaultNullDirection()) {
            return this.variable.toString();
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append(this.variable).append(" ").append(this.direction.shortString);
            if (this.nullDirection != this.direction.defaultNullDirection()) {
                sb.append(" ").append(this.nullDirection);
            }

            return sb.toString();
        }
    }

    @Override
    public String shortString() {
        if (this.nullDirection == this.direction.defaultNullDirection()) {
            return this.direction.shortString;
        } else {
            switch (this.nullDirection) {
                case FIRST:
                    return this.direction.shortString + "-nulls-first";
                case LAST:
                    return this.direction.shortString + "-nulls-last";
                default:
                    return this.direction.shortString;
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        GraphFieldCollation that = (GraphFieldCollation) o;
        return Objects.equals(variable, that.variable);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), variable);
    }
}
