package com.alibaba.graphscope.gremlin.transform;

public class WherePredicateValue {
    private String predicateValue;

    public WherePredicateValue(String predicateValue) {
        this.predicateValue = predicateValue;
    }

    @Override
    public String toString() {
        return predicateValue;
    }
}
