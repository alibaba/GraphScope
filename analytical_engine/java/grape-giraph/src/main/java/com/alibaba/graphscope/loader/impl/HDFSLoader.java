package com.alibaba.graphscope.loader.impl;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.net.URLClassLoader;

public class HDFSLoader extends AbstractLoader {

    public HDFSLoader(int id, URLClassLoader classLoader) {
        super(id, classLoader);
    }

    @Override
    protected AbstractVertexLoaderCallable createVertexLoaderCallable(int i, String inputPath, long min, long min1) {
        return new HDFSVertexLoaderCallable(i, inputPath, min, min1);
    }

    @Override
    protected AbstractEdgeLoaderCallable createEdgeLoaderCallable(int i, String inputPath, long min, long min1) {
        return new HDFSEdgeLoaderCallable(i, inputPath, min, min1);
    }


    @Override
    public TYPE loaderType() {
        return TYPE.HDFSLoader;
    }

    public class HDFSVertexLoaderCallable extends AbstractVertexLoaderCallable {
        @Override
        BufferedReader createBufferedReader(String inputPath) throws FileNotFoundException {
            return null;
        }

        public HDFSVertexLoaderCallable(int id, String inputPath, long min, long min1) {
            super(id, inputPath, min, min1);
        }
    }

    public class HDFSEdgeLoaderCallable extends AbstractEdgeLoaderCallable {
        @Override
        BufferedReader createBufferedReader(String inputPath) throws FileNotFoundException {
            return null;
        }

        public HDFSEdgeLoaderCallable(int id, String inputPath, long min, long min1) {
            super(id, inputPath, min, min1);
        }
    }
}
