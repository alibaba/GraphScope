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

package com.alibaba.graphscope.common.intermediate.operator;

import com.alibaba.graphscope.common.exception.InterOpIllegalArgException;

import org.javatuples.Pair;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class ProjectOp extends InterOpBase {
    // List of Pair<expr, alias>
    private Optional<OpArg> exprWithAlias;

    public ProjectOp() {
        super();
        exprWithAlias = Optional.empty();
    }

    public Optional<OpArg> getExprWithAlias() {
        if (exprWithAlias.isPresent()) {
            Object arg = exprWithAlias.get().getArg();
            Function transform = exprWithAlias.get().getTransform();
            Function thenApply =
                    transform.andThen(
                            (Object o) -> {
                                List<Pair> exprList = (List<Pair>) o;
                                Optional<OpArg> aliasOpt = getAlias();
                                if (aliasOpt.isPresent()) {
                                    // replace with the query given alias
                                    if (exprList.size() == 1) {
                                        Pair firstEntry = exprList.get(0);
                                        exprList.set(
                                                0, firstEntry.setAt1(aliasOpt.get().applyArg()));
                                    }
                                    if (exprList.size() > 1) {
                                        throw new InterOpIllegalArgException(
                                                getClass(),
                                                "exprWithAlias",
                                                "multiple columns as a single alias is"
                                                        + " unsupported");
                                    }
                                }
                                return exprList;
                            });
            setExprWithAlias(new OpArg(arg, thenApply));
        }
        return exprWithAlias;
    }

    public void setExprWithAlias(OpArg projectExpr) {
        this.exprWithAlias = Optional.of(projectExpr);
    }
}
