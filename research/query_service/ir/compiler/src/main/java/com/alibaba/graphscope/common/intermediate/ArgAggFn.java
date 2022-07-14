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

package com.alibaba.graphscope.common.intermediate;

import com.alibaba.graphscope.common.jna.type.FfiAggOpt;
import com.alibaba.graphscope.common.jna.type.FfiAlias;
import com.alibaba.graphscope.common.jna.type.FfiVariable;
import com.google.common.base.Objects;

// represent AggFn of Group Value, as a variable of GroupOp
public class ArgAggFn {
    private FfiVariable.ByValue var;
    private FfiAggOpt aggregate;
    private FfiAlias.ByValue alias;

    public ArgAggFn(FfiAggOpt aggregate, FfiAlias.ByValue alias, FfiVariable.ByValue var) {
        this.aggregate = aggregate;
        this.alias = alias;
        this.var = var;
    }

    public ArgAggFn(FfiAggOpt aggregate, FfiAlias.ByValue alias) {
        this.aggregate = aggregate;
        this.alias = alias;
        // set to none by default
        this.var = ArgUtils.asFfiNoneVar();
    }

    public FfiVariable.ByValue getVar() {
        return var;
    }

    public FfiAggOpt getAggregate() {
        return aggregate;
    }

    public FfiAlias.ByValue getAlias() {
        return alias;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ArgAggFn argAggFn = (ArgAggFn) o;
        return Objects.equal(var, argAggFn.var)
                && aggregate == argAggFn.aggregate
                && Objects.equal(alias, argAggFn.alias);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(var, aggregate, alias);
    }
}
