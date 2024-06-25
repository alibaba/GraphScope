package com.alibaba.graphscope.loader.impl;

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
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URLClassLoader;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibaba.graphscope.loader.LoaderUtils.generateTypeInt;
import static com.alibaba.graphscope.loader.LoaderUtils.getNumLinesOfFile;
import static org.apache.giraph.utils.ReflectionUtils.getTypeArguments;

public class DefaultLoader implements LoaderBase {
    protected static AtomicInteger LOADER_ID = new AtomicInteger(0);
    protected static AtomicInteger V_CALLABLE_ID = new AtomicInteger(0);
    protected static AtomicInteger E_CALLABLE_ID = new AtomicInteger(0);
    private static Logger logger = LoggerFactory.getLogger(DefaultLoader.class);
    private static int BATCH_SIZE = 1024;
    protected int loaderId;
    protected int threadNum;
    protected int workerId;
    protected int workerNum;
    protected Class<? extends VertexInputFormat> vertexInputFormatClz;
    protected Class<? extends EdgeInputFormat> edgeInputFormatClz;
    protected VertexInputFormat vertexInputFormat;
    protected EdgeInputFormat edgeInputFormat;

    protected ExecutorService executor;

    protected Method createVertexReaderMethod;
    protected Method createEdgeReaderMethod;

    protected GraphDataBufferManager proxy;
    protected Field vertexIdField;
    protected Field vertexValueField;
    protected Field vertexEdgesField;
    protected Field VIFBufferedReaderField;
    protected Field EIFBufferedReaderField;
    protected InputSplit inputSplit =
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

    protected Configuration configuration = new Configuration();
    protected GiraphConfiguration giraphConfiguration = new GiraphConfiguration(configuration);
    protected TaskAttemptID taskAttemptID = new TaskAttemptID();
    protected TaskAttemptContext taskAttemptContext =
            new TaskAttemptContextImpl(configuration, taskAttemptID);

    protected Class<? extends WritableComparable> giraphOidClass;
    protected Class<? extends Writable> giraphVDataClass;
    protected Class<? extends Writable> giraphEDataClass;
    protected URLClassLoader classLoader;

    public DefaultLoader(int id, URLClassLoader classLoader) {
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

    @Override
    public int concurrency() {
        return threadNum;
    }

    @Override
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
                new GraphDataBufferManagerImpl(
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
    @Override
    public int loadVertices(String inputPath, String vformatClass)
            throws ExecutionException, InterruptedException, ClassNotFoundException, IOException {
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
        loadVerticesImpl(inputPath);

        // Finish output stream, such that offset == size;
        proxy.finishAdding();
        return generateTypeInt(giraphOidClass, giraphVDataClass, giraphEDataClass);
    }

    @Override
    public void loadEdges(String inputPath, String eformatClass)
            throws ExecutionException, InterruptedException, ClassNotFoundException, IOException {
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

    protected void loadVerticesImpl(String inputPath) throws ExecutionException, InterruptedException, IOException {
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
            AbstractVertexLoaderCallable vertexLoaderCallable =
                    new AbstractVertexLoaderCallable(i, inputPath, Math.min(cur, end), Math.min(cur + chunkSize, end));
            futures[i] = executor.submit(vertexLoaderCallable);
            cur += chunkSize;
        }

        long sum = 0;
        for (int i = 0; i < threadNum; ++i) {
            sum += (Long) futures[i].get();
        }
        logger.info("[vertices] worker {} loaded {} lines ", workerId, sum);
    }

    BufferedReader createBufferedReader(String inputPath) throws IOException {
        if (inputPath.startsWith("hdfs://")) {
            org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(inputPath);
            return new BufferedReader(new InputStreamReader(path.getFileSystem(new Configuration()).open(path)));
        } else {
            FileReader fileReader = new FileReader(inputPath);
            return new BufferedReader(fileReader);
        }
    }

    protected void loadEdgesImpl(String filePath) throws ExecutionException, InterruptedException, IOException {
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
            DefaultLoader.AbstractEdgeLoaderCallable edgeLoaderCallable =
                    new DefaultLoader.AbstractEdgeLoaderCallable(
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

    public class AbstractVertexLoaderCallable implements Callable<Long> {
        private int threadId;
        private int callableId;
        private BufferedReader bufferedReader;
        private long start;
        private long end; // exclusive
        private VertexReader vertexReader;

        public AbstractVertexLoaderCallable(int threadId, String inputPath, long startLine, long endLine) {
            callableId = V_CALLABLE_ID.getAndAdd(1);
            try {
                FileReader fileReader = new FileReader(inputPath);
//                bufferedReader = new BufferedReader(fileReader);
                bufferedReader = createBufferedReader(inputPath);
            } catch (IOException e) {
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
                    "Abstract loader {} creating vertex loader callable: {}, file : {}, reader {},"
                            + " thread id {}, from {} to {}",
                    DefaultLoader.this,
                    AbstractVertexLoaderCallable.this,
                    inputPath,
                    bufferedReader,
                    threadId,
                    startLine,
                    endLine);
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

    public class AbstractEdgeLoaderCallable implements Callable<Long> {
        private int threadId;
        private int callableId;
        private BufferedReader bufferedReader;
        private long start;
        private long end; // exclusive
        private EdgeReader edgeReader;

        public AbstractEdgeLoaderCallable(int threadId, String inputPath, long startLine, long endLine) {
            callableId = E_CALLABLE_ID.getAndAdd(1);
            try {

//                bufferedReader = new BufferedReader(fileReader);
                bufferedReader = createBufferedReader(inputPath);
            } catch (IOException e) {
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
                    DefaultLoader.this,
                    AbstractEdgeLoaderCallable.this,
                    inputPath,
                    bufferedReader,
                    threadId,
                    startLine,
                    endLine);
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
