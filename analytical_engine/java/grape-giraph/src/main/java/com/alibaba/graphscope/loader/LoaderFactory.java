package com.alibaba.graphscope.loader;

import com.alibaba.graphscope.loader.impl.FileLoader;
import com.alibaba.graphscope.loader.impl.HDFSLoader;

import java.net.URLClassLoader;

public class LoaderFactory {
    public static LoaderBase createLoader(LoaderBase.TYPE type, int id, URLClassLoader classLoader) {
        if (type == LoaderBase.TYPE.FileLoader) {
            return new FileLoader(id, classLoader);
        }
        if (type == LoaderBase.TYPE.HDFSLoader){
            return new HDFSLoader(id, classLoader);
        }
        throw new IllegalArgumentException("Unsupported loader type: " + type);
    }
}
