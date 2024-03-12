package com.alibaba.graphscope.common.antlr4;

import com.google.common.collect.Sets;

import org.apache.calcite.sql.validate.SqlValidatorUtil;

import java.util.Set;

public class ExprUniqueAliasInfer {
    private final Set<String> uniqueNameList;

    public ExprUniqueAliasInfer() {
        this.uniqueNameList = Sets.newHashSet();
    }

    public String infer() {
        String name;
        int j = 0;
        do {
            name = SqlValidatorUtil.EXPR_SUGGESTER.apply(null, j++, 0);
        } while (uniqueNameList.contains(name));
        uniqueNameList.add(name);
        return name;
    }
}
