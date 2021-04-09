package com.alibaba.maxgraph.v2.frontend.compiler.logical.function;


import com.alibaba.maxgraph.proto.v2.LogicalCompare;

import java.util.List;

public interface CompareFunction {

    List<LogicalCompare> getLogicalCompareList();
}
