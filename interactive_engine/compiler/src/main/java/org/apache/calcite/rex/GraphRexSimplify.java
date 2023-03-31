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

package org.apache.calcite.rex;

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.sql.SqlKind;
import org.checkerframework.checker.nullness.qual.Nullable;

public class GraphRexSimplify extends RexSimplify {
    public GraphRexSimplify(
            RexBuilder rexBuilder, RelOptPredicateList predicates, RexExecutor executor) {
        super(rexBuilder, predicates, executor);
    }

    /**
     * default implementation in calcite will try to simplify {@code AND} or {@code OR} conditions to single {@code SEARCH} condition,
     * i.e. a.age > 10 and a.age < 20 --> a.age SEARCH range(10, 20).
     * we override the function to skip the simplification for {@code SEARCH} operator is unsupported yet in physical layer.
     * @param e
     * @param unknownAs
     * @return
     */
    @Override
    RexNode simplify(RexNode e, RexUnknownAs unknownAs) {
        if (e.getKind() == SqlKind.AND || e.getKind() == SqlKind.OR) {
            return e;
        } else {
            return super.simplify(e, unknownAs);
        }
    }

    @Override
    public @Nullable RexNode simplifyFilterPredicates(Iterable<? extends RexNode> predicates) {
        RexNode simplifiedAnds =
                simplifyUnknownAsFalse(RexUtil.composeConjunction(this.rexBuilder, predicates));
        return simplifiedAnds.isAlwaysFalse() ? null : this.removeNullabilityCast(simplifiedAnds);
    }
}
