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
public class FileLoader implements LoaderBase {
    private static Logger logger = LoggerFactory.getLogger(FileLoader.class);

    private static AtomicInteger LOADER_ID = new AtomicInteger(0);
    private static AtomicInteger V_CALLABLE_ID = new AtomicInteger(0);
    private static AtomicInteger E_CALLABLE_ID = new AtomicInteger(0);

    private int loaderId;
    private int threadNum;
    private int workerId;
    private int workerNum;
    private Class<? extends VertexInputFormat> vertexInputFormatClz;
    private Class<? extends EdgeInputFormat> edgeInputFormatClz;
    //    private Class<? extends VertexReader> vertexReaderClz;
    private VertexInputFormat vertexInputFormat;
    private EdgeInputFormat edgeInputFormat;

    private ExecutorService executor;
    //    private static String inputPath;

    private Method createVertexReaderMethod;
    private Method createEdgeReaderMethod;

    private GraphDataBufferManager proxy;
    private Field vertexIdField;
    private Field vertexValueField;
    private Field vertexEdgesField;
    private Field VIFBufferedReaderField;
    private Field EIFBufferedReaderField;
    private InputSplit inputSplit =
            new InputSplit() {
                @Override
                public long getLength() throws IOException, InterruptedException {
                    return 0;
                }

                @Override
                public String[] getLocations() throws IOException, InterruptedException {
                    return new String[0];
                }
            };
    private Configuration configuration = new Configuration();
    private GiraphConfiguration giraphConfiguration = new GiraphConfiguration(configuration);
    private TaskAttemptID taskAttemptID = new TaskAttemptID();
    private TaskAttemptContext taskAttemptContext =
            new TaskAttemptContext(configuration, taskAttemptID);

    private Class<? extends WritableComparable> giraphOidClass;
    private Class<? extends Writable> giraphVDataClass;
    private Class<? extends Writable> giraphEDataClass;
    private URLClassLoader classLoader;

    public FileLoader(int id, URLClassLoader classLoader) {
        this.classLoader = classLoader;
        logger.info("FileLoader using classLoader {} to load vif and eif", classLoader);
        this.giraphConfiguration.setClassLoader(this.classLoader);
        loaderId = id;
        try {
            vertexIdField = VertexImpl.class.getDeclaredField("initializeOid");
            vertexIdField.setAccessible(true);
            vertexValueField = VertexImpl.class.getDeclaredField("initializeVdata");
            vertexValueField.setAccessible(true);
            vertexEdgesField = VertexImpl.class.getDeclaredField("initializeEdges");
            vertexEdgesField.setAccessible(true);
            VIFBufferedReaderField = TextVertexInputFormat.class.getDeclaredField("fileReader");
            VIFBufferedReaderField.setAccessible(true);
            EIFBufferedReaderField = TextEdgeInputFormat.class.getDeclaredField("fileReader");
            EIFBufferedReaderField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new IllegalStateException(e.getMessage());
        }
    }

    public static FileLoader create(URLClassLoader cl) {
        synchronized (FileLoader.class) {
            return new FileLoader(LOADER_ID.getAndAdd(1), cl);
        }
    }

    public void init(
            int workerId,
            int workerNum,
            int threadNum,
            FFIByteVecVector vidBuffers,
            FFIByteVecVector vertexDataBuffers,
            FFIByteVecVector edgeSrcIdBuffers,
            FFIByteVecVector edgeDstIdBuffers,
            FFIByteVecVector edgeDataBuffers,
            FFIIntVecVector vidOffsets,
            FFIIntVecVector vertexDataOffsets,
            FFIIntVecVector edgeSrcIdOffsets,
            FFIIntVecVector edgeDstIdOffsets,
            FFIIntVecVector edgeDataOffsets) {
        this.workerId = workerId;
        this.workerNum = workerNum;
        logger.info("worker id: {}, worker num: {}", workerId, workerNum);

        this.threadNum = threadNum;
        this.executor = Executors.newFixedThreadPool(threadNum);
        // Create a proxy form adding vertex and adding edges
        proxy =
                new GraphDataBufferManangerImpl(
                        workerId,
                        threadNum,
                        vidBuffers,
                        vertexDataBuffers,
                        edgeSrcIdBuffers,
                        edgeDstIdBuffers,
                        edgeDataBuffers,
                        vidOffsets,
                        vertexDataOffsets,
                        edgeSrcIdOffsets,
                        edgeDstIdOffsets,
                        edgeDataOffsets);
    }

    /**
     * @param inputPath
     * @return Return an integer contains type params info.
     */
    public int loadVerticesAndEdges(String inputPath, String vformatClass)
            throws ExecutionException, InterruptedException, ClassNotFoundException {
        logger.info("vertex input path {}, vformat class{}", inputPath, vformatClass.toString());
        giraphConfiguration.setVertexInputFormatClass(
                (Class<? extends VertexInputFormat>) this.classLoader.loadClass(vformatClass));
        ImmutableClassesGiraphConfiguration conf =
                new ImmutableClassesGiraphConfiguration(giraphConfiguration);
        conf.setClassLoader(this.classLoader);
        try {
            vertexInputFormatClz = conf.getVertexInputFormatClass();

            inferGiraphTypesFromJSON(vertexInputFormatClz);

            vertexInputFormat = vertexInputFormatClz.newInstance();
            vertexInputFormat.setConf(conf);
            createVertexReaderMethod =
                    vertexInputFormatClz.getDeclaredMethod(
                            "createVertexReader", InputSplit.class, TaskAttemptContext.class);

        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
        loadVertices(inputPath);

        // Finish output stream, such that offset == size;
        proxy.finishAdding();
        return generateTypeInt(giraphOidClass, giraphVDataClass, giraphEDataClass);
    }

    public void loadEdges(String inputPath, String eformatClass)
            throws ExecutionException, InterruptedException, ClassNotFoundException {
        logger.debug("edge input path {}", inputPath);
        giraphConfiguration.setEdgeInputFormatClass(
                (Class<? extends EdgeInputFormat>) this.classLoader.loadClass(eformatClass));

        ImmutableClassesGiraphConfiguration conf =
                new ImmutableClassesGiraphConfiguration(giraphConfiguration);
        try {
            edgeInputFormatClz = conf.getEdgeInputFormatClass();

            edgeInputFormat = edgeInputFormatClz.newInstance();
            edgeInputFormat.setConf(conf);
            createEdgeReaderMethod =
                    edgeInputFormatClz.getDeclaredMethod(
                            "createEdgeReader", InputSplit.class, TaskAttemptContext.class);

        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
        loadEdgesImpl(inputPath);

        // Finish output stream, such that offset == size;
        proxy.finishAdding();
    }

    private void loadVertices(String inputPath) throws ExecutionException, InterruptedException {
        // Try to get number of lines
        long numOfLines = getNumLinesOfFile(inputPath);
        logger.info(
                "file {} has {} lines, workerId {}, workerNum {}",
                inputPath,
                numOfLines,
                workerId,
                workerNum);
        long linesPerWorker = (numOfLines + (workerNum - 1)) / workerNum;
        long start = Math.min(linesPerWorker * workerId, numOfLines);
        long end = Math.min(linesPerWorker * (workerId + 1), numOfLines);
        long chunkSize = (end - start + threadNum - 1) / threadNum;
        proxy.reserveNumVertices((int) (end - start));
        logger.debug(
                "[reading vertex] total lines {}, worker {} read {}, thread num {}, chunkSize {}",
                numOfLines,
                workerId,
                end - start,
                threadNum,
                chunkSize);
        long cur = start;

        Future[] futures = new Future[threadNum];

        for (int i = 0; i < threadNum; ++i) {
            VertexLoaderCallable vertexLoaderCallable =
                    new VertexLoaderCallable(
                            i, inputPath, Math.min(cur, end), Math.min(cur + chunkSize, end));
            futures[i] = executor.submit(vertexLoaderCallable);
            cur += chunkSize;
        }

        long sum = 0;
        for (int i = 0; i < threadNum; ++i) {
            sum += (Long) futures[i].get();
        }
        logger.info("[vertices] worker {} loaded {} lines ", workerId, sum);
    }

    private void loadEdgesImpl(String filePath) throws ExecutionException, InterruptedException {
        // Try to get number of lines
        long numOfLines = getNumLinesOfFile(filePath);
        long linesPerWorker = (numOfLines + (workerNum - 1)) / workerNum;
        long start = Math.min(linesPerWorker * workerId, numOfLines);
        long end = Math.min(linesPerWorker * (workerId + 1), numOfLines);
        long chunkSize = (end - start + threadNum - 1) / threadNum;
        proxy.reserveNumEdges((int) (end - start));
        logger.debug(
                "[reading edge] total lines {}, worker {} read {}, thread num {}, chunkSize {}",
                numOfLines,
                workerId,
                end - start,
                threadNum,
                chunkSize);
        long cur = start;

        Future[] futures = new Future[threadNum];

        for (int i = 0; i < threadNum; ++i) {
            EdgeLoaderCallable edgeLoaderCallable =
                    new EdgeLoaderCallable(
                            i, filePath, Math.min(cur, end), Math.min(cur + chunkSize, end));
            futures[i] = executor.submit(edgeLoaderCallable);
            cur += chunkSize;
        }

        long sum = 0;
        for (int i = 0; i < threadNum; ++i) {
            sum += (Long) futures[i].get();
        }
        logger.info("[edges] worker {} loaded {} lines ", workerId, sum);
    }

    @Override
    public LoaderBase.TYPE loaderType() {
        return TYPE.FileLoader;
    }

    @Override
    public int concurrency() {
        return threadNum;
    }

    private void inferGiraphTypesFromJSON(Class<? extends VertexInputFormat> child) {
        Class<?>[] classList = getTypeArguments(VertexInputFormat.class, child);
        Preconditions.checkArgument(classList.length == 3);
        giraphOidClass = (Class<? extends WritableComparable>) classList[0];
        giraphVDataClass = (Class<? extends Writable>) classList[1];
        giraphEDataClass = (Class<? extends Writable>) classList[2];
        logger.info(
                "infer from json params: oid {}, vdata {}, edata {}",
                giraphOidClass.getName(),
                giraphVDataClass.getName(),
                giraphEDataClass.getName());
    }

    @Override
    public String toString() {
        return FileLoader.class.toString() + "@" + loaderId;
    }

    class VertexLoaderCallable implements Callable<Long> {
        private int threadId;
        private int callableId;
        private BufferedReader bufferedReader;
        private long start;
        private long end; // exclusive
        private VertexReader vertexReader;

        public VertexLoaderCallable(int threadId, String inputPath, long startLine, long endLine) {
            callableId = V_CALLABLE_ID.getAndAdd(1);
            try {
                FileReader fileReader = new FileReader(inputPath);
                bufferedReader = new BufferedReader(fileReader);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }

            try {
                // create vertex reader
                vertexReader =
                        (VertexReader)
                                createVertexReaderMethod.invoke(
                                        vertexInputFormat, inputSplit, taskAttemptContext);
                logger.info("vertex reader: " + vertexReader);
                //                vertexReaderClz = vertexReader.getClass();
            } catch (Exception e) {
                e.printStackTrace();
            }

            this.threadId = threadId;
            this.start = startLine;
            this.end = endLine;
            //            proxy.reserveNumVertices((int) this.end - (int) this.start);
            logger.info(
                    "File loader {} creating vertex loader callable: {}, file : {}, reader {},"
                            + " thread id {}, from {} to {}",
                    FileLoader.this,
                    VertexLoaderCallable.this,
                    inputPath,
                    bufferedReader,
                    threadId,
                    startLine,
                    endLine);
        }

        @Override
        public String toString() {
            return VertexLoaderCallable.class.toString() + "@" + callableId;
        }

        /**
         * Computes a result, or throws an exception if unable to do so.
         *
         * @return computed result
         * @throws Exception if unable to compute a result
         */
        @Override
        public Long call() throws Exception {
            long cnt = 0;
            while (cnt < start) {
                bufferedReader.readLine();
                cnt += 1;
            }
            logger.info("worker {} thread {} skipped lines {}", workerId, threadId, cnt);
            // For text vertex reader, we set the data source manually.
            VIFBufferedReaderField.set(vertexInputFormat, bufferedReader);
            logger.info(
                    "worker {} thread {} has set the field {} to {}",
                    workerId,
                    threadId,
                    VIFBufferedReaderField,
                    bufferedReader);
            vertexReader.initialize(inputSplit, taskAttemptContext);
            vertexReader.setConf(vertexInputFormat.getConf());

            while (cnt < end && vertexReader.nextVertex()) {
                Vertex vertex = vertexReader.getCurrentVertex();
                Writable vertexId = (Writable) vertexIdField.get(vertex);
                Writable vertexValue = (Writable) vertexValueField.get(vertex);
                Iterable<Edge> vertexEdges = (Iterable<Edge>) vertexEdgesField.get(vertex);
                proxy.addVertex(threadId, vertexId, vertexValue);
                // suppose directed.
                proxy.addEdges(threadId, vertexId, vertexEdges);
                cnt += 1;
            }

            bufferedReader.close();

            return cnt - start;
        }
    }

    class EdgeLoaderCallable implements Callable<Long> {
        private int threadId;
        private int callableId;
        private BufferedReader bufferedReader;
        private long start;
        private long end; // exclusive
        private EdgeReader edgeReader;

        public EdgeLoaderCallable(int threadId, String inputPath, long startLine, long endLine) {
            callableId = E_CALLABLE_ID.getAndAdd(1);
            try {
                FileReader fileReader = new FileReader(inputPath);
                bufferedReader = new BufferedReader(fileReader);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }

            try {
                // create vertex reader
                edgeReader =
                        (EdgeReader)
                                createEdgeReaderMethod.invoke(
                                        edgeInputFormat, inputSplit, taskAttemptContext);
                logger.info("edge reader: " + edgeReader);
                //                vertexReaderClz = edgeReader.getClass();
            } catch (Exception e) {
                e.printStackTrace();
            }

            this.threadId = threadId;
            this.start = startLine;
            this.end = endLine;
            //            proxy.reserveNumVertices((int) this.end - (int) this.start);
            logger.info(
                    "File loader {} creating edge callable: {}, file : {}, reader {}, thread id {},"
                            + " from {} to {}",
                    FileLoader.this,
                    EdgeLoaderCallable.this,
                    inputPath,
                    bufferedReader,
                    threadId,
                    startLine,
                    endLine);
        }

        @Override
        public String toString() {
            return EdgeLoaderCallable.class.toString() + "@" + callableId;
        }

        /**
         * Computes a result, or throws an exception if unable to do so.
         *
         * @return computed result
         * @throws Exception if unable to compute a result
         */
        @Override
        public Long call() throws Exception {
            long cnt = 0;
            while (cnt < start) {
                bufferedReader.readLine();
                cnt += 1;
            }
            logger.info("worker {} thread {} skipped lines {}", workerId, threadId, cnt);
            // For text vertex reader, we set the data source manually.
            EIFBufferedReaderField.set(edgeInputFormat, bufferedReader);
            logger.info(
                    "worker {} thread {} has set the field {} to {}",
                    workerId,
                    threadId,
                    EIFBufferedReaderField,
                    bufferedReader);
            edgeReader.initialize(inputSplit, taskAttemptContext);
            edgeReader.setConf(edgeInputFormat.getConf());

            while (cnt < end && edgeReader.nextEdge()) {
                WritableComparable sourceId = edgeReader.getCurrentSourceId();
                Edge edge = edgeReader.getCurrentEdge();
                proxy.addEdge(threadId, sourceId, edge.getTargetVertexId(), edge.getValue());
                cnt += 1;
            }

            bufferedReader.close();
            return cnt - start;
        }
    }
}
