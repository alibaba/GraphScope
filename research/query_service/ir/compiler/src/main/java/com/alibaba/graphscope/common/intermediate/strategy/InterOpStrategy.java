package com.alibaba.graphscope.common.intermediate.strategy;

import com.alibaba.graphscope.common.intermediate.InterOpCollection;

public interface InterOpStrategy {
    void apply(InterOpCollection opCollection);
}
