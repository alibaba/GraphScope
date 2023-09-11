package com.alibaba.graphscope.common.ir.rel.metadata.glogue;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.Pattern;

public abstract class GlogueEdge {
    public abstract Pattern getDstPattern();

    public abstract Pattern getSrcPattern();
}
