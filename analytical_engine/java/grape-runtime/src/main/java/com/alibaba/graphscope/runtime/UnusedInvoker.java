package com.alibaba.graphscope.runtime;

import com.alibaba.graphscope.annotation.GrapeSkip;

@GrapeSkip(
    vertexDataTypes = {"Long", "Double", "Integer", "String", "Empty"},
    edgeDataTypes = {"Long", "Double", "Integer", "String", "Empty"},
    msgDataTypes = {"Long", "Double", "Integer", "String", "Empty"})
public class UnusedInvoker {}
