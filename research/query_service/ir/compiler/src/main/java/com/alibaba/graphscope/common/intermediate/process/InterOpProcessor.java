package com.alibaba.graphscope.common.intermediate.process;

import com.alibaba.graphscope.common.intermediate.InterOpCollection;

public interface InterOpProcessor {
    void process(InterOpCollection opCollection);
}
