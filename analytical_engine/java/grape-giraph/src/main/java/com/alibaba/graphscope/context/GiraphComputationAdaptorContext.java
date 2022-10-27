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
package com.alibaba.graphscope.context;

import static com.alibaba.graphscope.parallel.utils.Utils.getAllHostNames;

import static org.apache.giraph.conf.GiraphConstants.MESSAGE_MANAGER_TYPE;
import static org.apache.giraph.job.HadoopUtils.makeTaskAttemptContext;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.communication.Communicator;
import com.alibaba.graphscope.ds.GSVertexArray;
import com.alibaba.graphscope.factory.GiraphComputationFactory;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.graph.AggregatorManager;
import com.alibaba.graphscope.graph.GiraphEdgeManager;
import com.alibaba.graphscope.graph.GiraphVertexIdManager;
import com.alibaba.graphscope.graph.VertexDataManager;
import com.alibaba.graphscope.graph.VertexFactory;
import com.alibaba.graphscope.graph.impl.AggregatorManagerImpl;
import com.alibaba.graphscope.graph.impl.VertexImpl;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.parallel.mm.GiraphMessageManager;
import com.alibaba.graphscope.parallel.mm.GiraphMessageManagerFactory;
import com.alibaba.graphscope.parallel.utils.NetworkMap;
import com.alibaba.graphscope.serialization.FFIByteVectorInputStream;
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import com.alibaba.graphscope.utils.ConfigurationUtils;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.VertexWriter;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.master.SuperstepClasses;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URLClassLoader;
import java.util.BitSet;
import java.util.Objects;

/**
 * Generic adaptor context class. The type parameter OID,VID_VDATA_T,EDATA_T is irrelevant to User
 * writables. They are type parameters for grape fragment. We need them to support multiple set of
 * actual type parameters in one adaptor.
 *
 * @param <OID_T>
 * @param <VID_T>
 * @param <VDATA_T>
 * @param <EDATA_T>
 */
public class GiraphComputationAdaptorContext<OID_T, VID_T, VDATA_T, EDATA_T>
        extends VertexDataContext<IFragment<OID_T, VID_T, VDATA_T, EDATA_T>, VDATA_T>
        implements DefaultContextBase<OID_T, VID_T, VDATA_T, EDATA_T> {

    private static Logger logger = LoggerFactory.getLogger(GiraphComputationAdaptorContext.class);
    public VertexImpl vertex;
    private long innerVerticesNum;
    private long fragVerticesNum;
    private URLClassLoader classLoader;
    private WorkerContext workerContext;
    /**
     * Only executed by the master, in our case, the coordinator worker in mpi world
     */
    private MasterCompute masterCompute;

    private AbstractComputation userComputation;
    private GiraphMessageManager giraphMessageManager;

    /**
     * Manages the vertex original id.
     */
    private GiraphVertexIdManager vertexIdManager;
    /**
     * Manages the vertex data.
     */
    private VertexDataManager vertexDataManager;

    /**
     * Edge manager. We can choose to use a immutable edge manager or others.
     */
    private GiraphEdgeManager edgeManager;

    /**
     * Aggregator manager.
     */
    private AggregatorManager aggregatorManager;

    // A wrapper for FFICommunicator. should set via reflection.
    // MUST be create here, in object creation, not initing.
    private Communicator communicator = new Communicator();

    // Place to set back vertex values.
    private GSVertexArray vertexArray;
    private BitSet halted;

    private SuperstepClasses superstepClasses;

    public AbstractComputation getUserComputation() {
        return userComputation;
    }

    public GiraphMessageManager getGiraphMessageManager() {
        return giraphMessageManager;
    }

    public WorkerContext getWorkerContext() {
        return workerContext;
    }

    public boolean hasMasterCompute() {
        return Objects.nonNull(masterCompute);
    }

    public MasterCompute getMasterCompute() {
        return masterCompute;
    }

    public AggregatorManager getAggregatorManager() {
        return aggregatorManager;
    }

    public void setClassLoader(URLClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    public void writeBackVertexData() {
        ImmutableClassesGiraphConfiguration conf = userComputation.getConf();
        logger.info("Writing back vertex data of type back to c++ context ");
        if (Objects.isNull(vertexDataManager)) {
            throw new IllegalStateException("expect a non null vertex data manager");
        }
        //        GSVertexArray<VDATA_T> contextVdata = data();
        if (Objects.isNull(vertexArray) || vertexArray.getAddress() == 0) {
            throw new IllegalStateException("GS vertex array empty");
        }
        // Generate a byte stream contains all vertex data.
        FFIByteVectorOutputStream outputStream = new FFIByteVectorOutputStream();
        long[] offsets = new long[(int) innerVerticesNum];
        long maxOffset = 0;
        {
            long previous = 0;
            try {
                if (conf.getGrapeVdataClass().equals(String.class)) {
                    for (long lid = 0; lid < innerVerticesNum; ++lid) {
                        vertexDataManager.getVertexData(lid).write(outputStream);
                        long cur = outputStream.bytesWriten();
                        offsets[(int) lid] = cur - previous;
                        maxOffset = Math.max(offsets[(int) lid], maxOffset);
                        previous = cur;
                    }
                } else {
                    for (long lid = 0; lid < innerVerticesNum; ++lid) {
                        vertexDataManager.getVertexData(lid).write(outputStream);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        outputStream.finishSetting();
        FFIByteVector vector = outputStream.getVector();
        FFIByteVectorInputStream inputStream = new FFIByteVectorInputStream(vector);

        com.alibaba.graphscope.ds.Vertex<VID_T> grapeVertex =
                (com.alibaba.graphscope.ds.Vertex<VID_T>)
                        FFITypeFactoryhelper.newVertex(conf.getGrapeVidClass());
        try {
            if (conf.getGrapeVdataClass().equals(Long.class)) {
                for (long lid = 0; lid < innerVerticesNum; ++lid) {
                    grapeVertex.SetValue((VID_T) (Long) lid);
                    if (inputStream.longAvailable() <= 0) {
                        throw new IllegalStateException(
                                "Input stream too short for " + innerVerticesNum + " vertices");
                    }
                    long value = inputStream.readLong();
                    vertexArray.setValue(grapeVertex, value);
                }
            } else if (conf.getGrapeVdataClass().equals(Integer.class)) {
                for (long lid = 0; lid < innerVerticesNum; ++lid) {
                    grapeVertex.SetValue((VID_T) (Long) lid);
                    if (inputStream.longAvailable() <= 0) {
                        throw new IllegalStateException(
                                "Input stream too short for " + innerVerticesNum + " vertices");
                    }
                    int value = inputStream.readInt();
                    vertexArray.setValue(grapeVertex, value);
                }
            } else if (conf.getGrapeVdataClass().equals(Double.class)) {
                for (long lid = 0; lid < innerVerticesNum; ++lid) {
                    grapeVertex.SetValue((VID_T) (Long) lid);
                    if (inputStream.longAvailable() <= 0) {
                        throw new IllegalStateException(
                                "Input stream too short for " + innerVerticesNum + " vertices");
                    }
                    double value = inputStream.readDouble();
                    vertexArray.setValue(grapeVertex, value);
                }
            } else if (conf.getGrapeVdataClass().equals(Float.class)) {
                for (long lid = 0; lid < innerVerticesNum; ++lid) {
                    grapeVertex.SetValue((VID_T) (Long) lid);
                    if (inputStream.longAvailable() <= 0) {
                        throw new IllegalStateException(
                                "Input stream too short for " + innerVerticesNum + " vertices");
                    }
                    float value = inputStream.readFloat();
                    vertexArray.setValue(grapeVertex, value);
                }
            } else if (conf.getGrapeVdataClass().equals(String.class)) {
                byte[] bytes = new byte[(int) maxOffset];
                for (long lid = 0; lid < innerVerticesNum; ++lid) {
                    grapeVertex.SetValue((VID_T) (Long) lid);
                    if (inputStream.longAvailable() <= 0) {
                        throw new IllegalStateException(
                                "Input stream too short for " + innerVerticesNum + " vertices");
                    }
                    if (inputStream.read(bytes, 0, (int) offsets[(int) lid]) == -1) {
                        throw new IllegalStateException("read input stream failed");
                    }
                    // This string is not readable.
                    vertexArray.setValue(grapeVertex, new String(bytes));
                }
            } else {
                throw new IllegalStateException(
                        "Unrecognized vdata class:" + conf.getGrapeVdataClass().getName());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * <em>CAUTION: THIS METHOD SHALL BE CALLED in JNI after initCommunicator.</em>.
     *
     * <p>With communicatorAddress passed, we init a FFICommunicator obj with that address.
     */
    @Override
    public void Init(
            IFragment<OID_T, VID_T, VDATA_T, EDATA_T> frag,
            DefaultMessageManager messageManager,
            JSONObject jsonObject) {

        /** Construct a configuration obj. */
        ImmutableClassesGiraphConfiguration conf = generateConfiguration(jsonObject, frag);

        superstepClasses = SuperstepClasses.createAndExtractTypes(conf);

        if (jsonObject.containsKey("vineyard_id")) {
            logger.info("vineyard id");
        } else {
            logger.info("no vineyard id provided!");
        }

        // We want the final result be put in c++ vertexDataContext.
        createFFIContext(frag, conf.getGrapeVdataClass(), false);
        // Hold the returned vertex array.
        vertexArray = data();
        if (Objects.isNull(vertexArray) || vertexArray.getAddress() == 0) {
            logger.error("vertex array empty");
        }

        userComputation =
                (AbstractComputation) ReflectionUtils.newInstance(conf.getComputationClass());
        userComputation.setFragment(frag);
        userComputation.setConf(conf);

        logger.info("Created user computation class: " + userComputation.getClass().getName());
        innerVerticesNum = frag.getInnerVerticesNum();
        fragVerticesNum = (Long) frag.getVerticesNum();

        /**
         * Important, we don't provided any constructors for workerContext, so make sure all fields
         * has been carefully set.
         */
        workerContext = conf.createWorkerContext();
        //        workerContext.setCommunicator(communicator);
        workerContext.setFragment(frag);
        workerContext.setCurStep(0);
        userComputation.setWorkerContext(workerContext);

        // halt array to mark active
        halted = new BitSet((int) frag.getInnerVerticesNum());

        // Init vertex data/oid manager
        // vertex data and vertex id manager should contains out vertices.
        vertexDataManager =
                GiraphComputationFactory.createDefaultVertexDataManager(
                        conf, frag, innerVerticesNum);
        vertexIdManager =
                GiraphComputationFactory.createDefaultVertexIdManager(conf, frag, fragVerticesNum);
        edgeManager =
                GiraphComputationFactory.createImmutableEdgeManager(conf, frag, vertexIdManager);

        vertex =
                VertexFactory.createDefaultVertex(
                        conf.getGrapeVidClass(),
                        conf.getVertexIdClass(),
                        conf.getVertexValueClass(),
                        conf.getEdgeValueClass(),
                        this);
        vertex.setVertexIdManager(vertexIdManager);
        vertex.setVertexDataManager(vertexDataManager);
        vertex.setEdgeManager(edgeManager);

        // VertexIdManager is needed since we need oid <-> lid converting.
        String giraphMessageManagerType = System.getenv("MESSAGE_MANAGER_TYPE");
        if (Objects.isNull(giraphMessageManagerType) || giraphMessageManagerType.isEmpty()) {
            giraphMessageManagerType = MESSAGE_MANAGER_TYPE.get(conf);
        }
        // Here, communicator could be have no FFICommunicator instance.
        if (giraphMessageManagerType.equals("netty")) {
            String[] allHostsNames =
                    getAllHostNames(
                            conf.getWorkerId(),
                            conf.getWorkerNum(),
                            communicator.getFFICommunicator());
            logger.info(String.join(",", allHostsNames));
            NetworkMap networkMap =
                    new NetworkMap(
                            conf.getWorkerId(),
                            conf.getWorkerNum(),
                            conf.getMessagerInitServerPort(),
                            allHostsNames);
            giraphMessageManager =
                    GiraphMessageManagerFactory.create(
                            "netty",
                            frag,
                            null,
                            networkMap,
                            conf,
                            communicator.getFFICommunicator(),
                            vertexIdManager);
        } else if (giraphMessageManagerType.equals("mpi")) {
            giraphMessageManager =
                    GiraphMessageManagerFactory.create(
                            "mpi",
                            frag,
                            messageManager,
                            null,
                            conf,
                            communicator.getFFICommunicator(),
                            vertexIdManager);
        } else {
            throw new IllegalStateException("Expect a netty or mpi messager");
        }
        userComputation.setGiraphMessageManager(giraphMessageManager);

        /** Aggregator manager, manages aggregation, reduce, broadcast */

        //        String masterWorkerIp = getMasterWorkerIp(frag.fid(), frag.fnum());

        aggregatorManager = new AggregatorManagerImpl(conf, frag.fid(), frag.fnum());
        //        aggregatorManager = new AggregatorManagerNettyImpl(conf, frag.fid(), frag.fnum());
        userComputation.setAggregatorManager(aggregatorManager);
        workerContext.setAggregatorManager(aggregatorManager);

        /** Create master compute if master compute is specified. */
        if (conf.getMasterComputeClass() != null) {
            masterCompute = conf.createMasterCompute();
            logger.info("Creating master compute class");
            try {
                masterCompute.setAggregatorManager(aggregatorManager);
                masterCompute.initialize();
                masterCompute.setFragment(frag);
                masterCompute.setConf(conf);
                //                masterCompute.setOutgoingMessageClasses();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
            logger.info("Finish master compute initialization.");
        } else {
            logger.info("No master compute class specified");
        }
    }

    /**
     * For giraph applications, we need to run postApplication method after all computation.
     *
     * @param frag The graph fragment contains the graph info.
     */
    @Override
    public void Output(IFragment<OID_T, VID_T, VDATA_T, EDATA_T> frag) {
        workerContext.postApplication();
        /** Closing netty client and server here. */
        giraphMessageManager.postApplication();
        ImmutableClassesGiraphConfiguration conf = userComputation.getConf();
        String filePath = conf.getDefaultWorkerFile() + "-frag-" + frag.fid();
        // Output vertices.
        if (conf.hasVertexOutputFormat()) {
            VertexOutputFormat vertexOutputFormat = conf.createWrappedVertexOutputFormat();
            vertexOutputFormat.setConf(conf);
            TaskAttemptContext taskAttemptContext = makeTaskAttemptContext(conf);
            vertexOutputFormat.preWriting(taskAttemptContext);

            {
                try {
                    VertexWriter vertexWriter =
                            vertexOutputFormat.createVertexWriter(taskAttemptContext);

                    vertexWriter.setConf(conf);
                    vertexWriter.initialize(taskAttemptContext);

                    // write vertex
                    Vertex vertex = conf.createVertex();
                    if (conf.getVertexIdClass().equals(VertexImpl.class)) {
                        logger.info("Cast to vertexImpl to output");
                        VertexImpl vertexImp = (VertexImpl) vertex;
                        vertexImp.setVertexIdManager(vertexIdManager);
                        vertexImp.setVertexDataManager(vertexDataManager);
                        vertexImp.setEdgeManager(edgeManager);
                        vertexImp.setConf(conf);
                        for (long i = 0; i < innerVerticesNum; ++i) {
                            vertexImp.setLocalId((int) i);
                            vertexWriter.writeVertex(vertexImp);
                        }
                    }

                    vertexWriter.close(taskAttemptContext);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            vertexOutputFormat.postWriting(taskAttemptContext);

        } else {
            logger.info("No vertex output class specified, output using default output logic");

            try {
                logger.info("Writing output to: " + filePath);
                FileWriter fileWritter = new FileWriter(new File(filePath));
                BufferedWriter bufferedWriter = new BufferedWriter(fileWritter);
                logger.debug("inner vertices: " + innerVerticesNum + frag.innerVertices().size());
                for (long i = 0; i < innerVerticesNum; ++i) {
                    bufferedWriter.write(
                            vertexIdManager.lid2Oid(i)
                                    + "\t"
                                    + vertexDataManager.getVertexData(i)
                                    + "\n");
                }
                bufferedWriter.close();
            } catch (Exception e) {
                logger.error("Exception in writing out: " + e.getMessage());
            }
        }

        // Copy data in vertexDataManager to vertexDataContext.data()

    }

    public void updateIncomingMessageClass(ImmutableClassesGiraphConfiguration conf) {
        conf.updateSuperstepClasses(superstepClasses);
    }

    public void haltVertex(long lid) {
        halted.set((int) lid);
    }

    public boolean isHalted(long lid) {
        return halted.get((int) lid);
    }

    public boolean allHalted() {
        return halted.cardinality() == innerVerticesNum;
    }

    public void activateVertex(long lid) {
        halted.set((int) lid, false);
    }

    /**
     * return a configuration instance with key-value pairs in params.
     *
     * @param params received params.
     */
    private ImmutableClassesGiraphConfiguration generateConfiguration(
            JSONObject params, IFragment fragment) {
        Configuration configuration = new Configuration();
        GiraphConfiguration giraphConfiguration = new GiraphConfiguration(configuration);
        if (Objects.isNull(classLoader)) {
            throw new IllegalStateException("Empty class loader");
        }
        giraphConfiguration.setClassLoader(classLoader);

        try {
            ConfigurationUtils.parseArgs(giraphConfiguration, params);
            //            ConfigurationUtils.parseJavaFragment(giraphConfiguration, fragment);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        ImmutableClassesGiraphConfiguration conf;
        if (params.containsKey("frag_name")) {
            String cppFragStr = params.getString("frag_name");
            conf =
                    new ImmutableClassesGiraphConfiguration(
                            giraphConfiguration, cppFragStr, fragment.fid(), fragment.fnum());
        } else {
            conf = new ImmutableClassesGiraphConfiguration<>(giraphConfiguration, fragment);
        }

        if (checkConsistency(conf)) {
            logger.info(
                    "Okay, the type parameters in user computation is consistent with fragment");
        } else {
            throw new IllegalStateException(
                    "User computation type parameters not consistent with fragment types");
        }
        return conf;
    }

    /**
     * Check whether user provided giraph app consistent with our fragment.
     *
     * @param configuration configuration
     * @return true if consistent.
     */
    private boolean checkConsistency(ImmutableClassesGiraphConfiguration configuration) {
        return ConfigurationUtils.checkTypeConsistency(
                        configuration.getGrapeOidClass(), configuration.getVertexIdClass())
                && ConfigurationUtils.checkTypeConsistency(
                        configuration.getGrapeEdataClass(), configuration.getEdgeValueClass())
                && ConfigurationUtils.checkTypeConsistency(
                        configuration.getGrapeVdataClass(), configuration.getVertexValueClass());
    }
}
