package com.alibaba.graphscope.common.intermediate;

import com.alibaba.graphscope.common.jna.type.FfiAggOpt;
import com.alibaba.graphscope.common.jna.type.FfiAlias;
import com.alibaba.graphscope.common.jna.type.FfiVariable;
import com.google.common.base.Objects;

import java.util.ArrayList;
import java.util.List;

// represent AggFn of Group Value, as a variable of GroupOp
public class ArgAggFn {
    private List<FfiVariable.ByValue> vars;
    private FfiAggOpt aggregate;
    private FfiAlias.ByValue alias;

    public ArgAggFn(FfiAggOpt aggregate, FfiAlias.ByValue alias) {
        this.aggregate = aggregate;
        this.alias = alias;
        this.vars = new ArrayList<>();
    }

    public void addVar(FfiVariable.ByValue var) {
        this.vars.add(var);
    }

    public List<FfiVariable.ByValue> getVars() {
        return vars;
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
        return Objects.equal(vars, argAggFn.vars) &&
                aggregate == argAggFn.aggregate &&
                Objects.equal(alias, argAggFn.alias);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(vars, aggregate, alias);
    }
}