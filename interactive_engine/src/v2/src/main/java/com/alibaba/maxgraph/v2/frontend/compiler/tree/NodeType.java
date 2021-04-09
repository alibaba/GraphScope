package com.alibaba.maxgraph.v2.frontend.compiler.tree;

public enum NodeType {
    SOURCE, MAP, FLATMAP, FILTER, UNION, AGGREGATE, AGGREGATE_LIST, REPEAT, BFSEND, STORE, GRAPH, BARRIER,
    LAMBDA_FILTER, LAMBDA_MAP, LAMBDA_FLATMAP
}
