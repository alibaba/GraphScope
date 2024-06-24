package com.alibaba.graphscope.loader.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
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
            //Expect a string with format: hdfs://host:port/path
            Path path = new Path(inputPath);
            try {
                return new BufferedReader(new InputStreamReader(path.getFileSystem(new Configuration()).open(path)));
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }

        public HDFSVertexLoaderCallable(int id, String inputPath, long min, long min1) {
            super(id, inputPath, min, min1);
        }

        @Override
        public String toString() {
            return HDFSVertexLoaderCallable.class.toString() + "@" + loaderId;
        }
    }

    public class HDFSEdgeLoaderCallable extends AbstractEdgeLoaderCallable {
        @Override
        BufferedReader createBufferedReader(String inputPath) throws FileNotFoundException {
            //Expect a string with format: hdfs://host:port/path
            Path path = new Path(inputPath);
            try {
                return new BufferedReader(new InputStreamReader(path.getFileSystem(new Configuration()).open(path)));
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }

        public HDFSEdgeLoaderCallable(int id, String inputPath, long min, long min1) {
            super(id, inputPath, min, min1);
        }

        @Override
        public String toString() {
            return HDFSEdgeLoaderCallable.class.toString() + "@" + loaderId;
        }
    }
}
