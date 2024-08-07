package com.alibaba.graphscope.loader;

import com.alibaba.graphscope.loader.impl.DefaultLoader;

import java.net.URLClassLoader;

public class LoaderFactory {
    public static LoaderBase createLoader(int id, URLClassLoader classLoader) {
        return new DefaultLoader(id, classLoader);
    }
}
