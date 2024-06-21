/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.loader.impl;

import static com.alibaba.graphscope.loader.LoaderUtils.generateTypeInt;
import static com.alibaba.graphscope.utils.FileUtils.getNumLinesOfFile;

import static org.apache.giraph.utils.ReflectionUtils.getTypeArguments;

import com.alibaba.graphscope.graph.impl.VertexImpl;
import com.alibaba.graphscope.loader.GraphDataBufferManager;
import com.alibaba.graphscope.loader.LoaderBase;
import com.alibaba.graphscope.stdcxx.FFIByteVecVector;
import com.alibaba.graphscope.stdcxx.FFIIntVecVector;
import com.google.common.base.Preconditions;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URLClassLoader;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Load from a file on system.
 */
public class FileLoader extends AbstractLoader {
    private static Logger logger = LoggerFactory.getLogger(FileLoader.class);

    public FileLoader(int id, URLClassLoader classLoader) {
        super(id, classLoader);
    }

    @Override
    protected AbstractVertexLoaderCallable createVertexLoaderCallable(int i, String inputPath, long min, long min1) {
        return new FileVertexLoaderCallable(i, inputPath, min, min1);
    }

    @Override
    protected AbstractEdgeLoaderCallable createEdgeLoaderCallable(int i, String inputPath, long min, long min1) {
        return new FileEdgeLoaderCallable(i, inputPath, min, min1);
    }


    @Override
    public LoaderBase.TYPE loaderType() {
        return TYPE.FileLoader;
    }

    @Override
    public String toString() {
        return FileLoader.class.toString() + "@" + loaderId;
    }


    public class FileVertexLoaderCallable extends AbstractVertexLoaderCallable {

        public FileVertexLoaderCallable(int id, String inputPath, long start, long end) {
            super(id,inputPath,start,end);
        }

        @Override
        BufferedReader createBufferedReader(String inputPath) throws FileNotFoundException {
            FileReader fileReader = new FileReader(inputPath);
            return new BufferedReader(fileReader);
        }

        @Override
        public String toString() {
            return FileVertexLoaderCallable.class.toString() + "@" + loaderId;
        }

    }

    public class FileEdgeLoaderCallable extends AbstractEdgeLoaderCallable {

        public FileEdgeLoaderCallable(int id, String inputPath, long start, long end) {
            super(id,inputPath,start,end);
        }

        @Override
        BufferedReader createBufferedReader(String inputPath) throws FileNotFoundException {
            FileReader fileReader = new FileReader(inputPath);
            return new BufferedReader(fileReader);
        }

        @Override
        public String toString() {
            return FileEdgeLoaderCallable.class.toString() + "@" + loaderId;
        }

    }


}
