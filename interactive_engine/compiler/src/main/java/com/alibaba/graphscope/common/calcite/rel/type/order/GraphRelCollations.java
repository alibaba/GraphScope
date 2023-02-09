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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.RelMultipleTrait;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;

import java.util.Iterator;
import java.util.List;

/**
 * provide static functions to create {@code GraphRelCollationImpl}
 * which wrappers all {@code RelFieldCollation}(s) for an order operator
 */
public abstract class GraphRelCollations {
    public static RelCollation of(List<RelFieldCollation> fieldCollations) {
        return new GraphRelCollationImpl(ImmutableList.copyOf(fieldCollations));
    }

    /**
     * extract all variables from a list of {@code RelFieldCollation}(s)
     * @param fieldCollations
     * @return
     */
    public static List<RexNode> variables(List<RelFieldCollation> fieldCollations) {
        return Util.transform(
                fieldCollations,
                (RelFieldCollation collation) -> ((GraphFieldCollation) collation).getVariable());
    }

    static class GraphRelCollationImpl implements RelCollation {
        private final ImmutableList<RelFieldCollation> fieldCollations;

        public GraphRelCollationImpl(ImmutableList<RelFieldCollation> fieldCollations) {
            this.fieldCollations = fieldCollations;
            Preconditions.checkArgument(
                    Util.isDistinct(variables(fieldCollations)), "variables must be distinct");
        }

        @Override
        public RelTraitDef getTraitDef() {
            return null;
        }

        @Override
        public boolean satisfies(RelTrait relTrait) {
            return false;
        }

        @Override
        public String toString() {
            Iterator<RelFieldCollation> it = getFieldCollations().iterator();
            if (!it.hasNext()) {
                return "[]";
            }
            StringBuilder sb = new StringBuilder();
            sb.append('[');
            for (; ; ) {
                sb.append(it.next());
                if (!it.hasNext()) {
                    return sb.append(']').toString();
                }
                sb.append(',').append(' ');
            }
        }

        @Override
        public void register(RelOptPlanner relOptPlanner) {}

        @Override
        public List<RelFieldCollation> getFieldCollations() {
            return this.fieldCollations;
        }

        @Override
        public boolean isTop() {
            return false;
        }

        @Override
        public int compareTo(RelMultipleTrait o) {
            return 0;
        }
    }
}
