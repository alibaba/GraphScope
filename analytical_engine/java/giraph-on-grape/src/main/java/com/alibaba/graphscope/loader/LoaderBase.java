package com.alibaba.graphscope.loader;

/**
 * Base interface defines behavior for a loader.
 */
public interface LoaderBase {

    enum TYPE{
        FileLoader,
    }
    LoaderBase.TYPE loaderType();

    int concurrency();
}
