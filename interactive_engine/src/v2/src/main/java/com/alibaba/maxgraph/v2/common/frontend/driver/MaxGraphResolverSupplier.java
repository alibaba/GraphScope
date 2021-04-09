package com.alibaba.maxgraph.v2.common.frontend.driver;

import org.apache.tinkerpop.shaded.kryo.ClassResolver;
import org.apache.tinkerpop.shaded.kryo.util.DefaultClassResolver;

import java.util.function.Supplier;

public class MaxGraphResolverSupplier implements Supplier<ClassResolver> {
    @Override
    public ClassResolver get() {
        return new DefaultClassResolver();
    }
}
