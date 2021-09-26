package com.alibaba.graphscope.context;

import static com.alibaba.graphscope.parallel.utils.Utils.getAllHostNames;

import static org.apache.giraph.conf.GiraphConstants.MESSAGE_MANAGER_TYPE;
import static org.apache.giraph.job.HadoopUtils.makeTaskAttemptContext;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.communication.Communicator;
import com.alibaba.graphscope.ds.GSVertexArray;
import com.alibaba.graphscope.factory.GiraphComputationFactory;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.parallel.mm.GiraphMessageManager;
import com.alibaba.graphscope.parallel.mm.GiraphMessageManagerFactory;
import com.alibaba.graphscope.parallel.utils.NetworkMap;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.AggregatorManager;
import org.apache.giraph.graph.EdgeManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexDataManager;
import org.apache.giraph.graph.VertexFactory;
import org.apache.giraph.graph.VertexIdManager;
import org.apache.giraph.graph.impl.AggregatorManagerImpl;
import org.apache.giraph.graph.impl.VertexImpl;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.VertexWriter;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.utils.ConfigurationUtils;
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
public class GiraphComputationAdaptorContext<OID_T, VID_T, VDATA_T, EDATA_T> extends VertexDataContext<IFragment<OID_T,VID_T,VDATA_T,EDATA_T>,VDATA_T>
        implements DefaultContextBase<OID_T, VID_T, VDATA_T, EDATA_T> {

    private static Logger logger = LoggerFactory.getLogger(GiraphComputationAdaptorContext.class);

    private long innerVerticesNum;

    private long fragVerticesNum;

    private WorkerContext workerContext;
    /** Only executed by the master, in our case, the coordinator worker in mpi world */
    private MasterCompute masterCompute;

    private AbstractComputation userComputation;
    public VertexImpl vertex;
    private GiraphMessageManager giraphMessageManager;

    /** Manages the vertex original id. */
    private VertexIdManager vertexIdManager;
    /** Manages the vertex data. */
    private VertexDataManager vertexDataManager;

    /** Edge manager. We can choose to use a immutable edge manager or others. */
    private EdgeManager edgeManager;

    /** Aggregator manager. */
    private AggregatorManager aggregatorManager;

    //A wrapper for FFICommunicator. should set via reflection.
    //MUST be create here, in object creation, not initing.
    private Communicator communicator = new Communicator();

    //Place to set back vertex values.
    private GSVertexArray vertexArray;

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

    private BitSet halted;

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

        if (jsonObject.containsKey("vineyard_id")){
            logger.info("vineyard id");
        }
        else {
            logger.info("no vineyard id provided!");
        }

        //We want the final result be put in c++ vertexDataContext.
        createFFIContext(frag, conf.getGrapeVdataClass(), false);
        //Hold the returned vertex array.
        vertexArray = data();
        if (Objects.isNull(vertexArray) || vertexArray.getAddress() == 0){
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
        //Here, communicator could be have no FFICommunicator instance.
        if (giraphMessageManagerType.equals("netty")) {
            String[] allHostsNames =
                    getAllHostNames(conf.getWorkerId(), conf.getWorkerNum(), communicator.getFFICommunicator());
            logger.info(String.join(",", allHostsNames));
            NetworkMap networkMap =
                    new NetworkMap(
                            conf.getWorkerId(),
                            conf.getWorkerNum(),
                            conf.getMessagerInitServerPort(),
                            allHostsNames);
            giraphMessageManager =
                    GiraphMessageManagerFactory.create(
                            "netty", frag, null, networkMap, conf, communicator.getFFICommunicator());
        } else if (giraphMessageManagerType.equals("mpi")) {
            giraphMessageManager =
                    GiraphMessageManagerFactory.create(
                            "mpi", frag, messageManager, null, conf, communicator.getFFICommunicator());
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
                            vertexIdManager.getId(i)
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

        try {
            ConfigurationUtils.parseArgs(giraphConfiguration, params);
            //            ConfigurationUtils.parseJavaFragment(giraphConfiguration, fragment);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        ImmutableClassesGiraphConfiguration conf;
        if (params.containsKey("frag_name")){
            String cppFragStr = params.getString("frag_name");
            conf = new ImmutableClassesGiraphConfiguration(giraphConfiguration, cppFragStr, fragment.fid(), fragment.fnum());
        }
        else {
            conf =
                new ImmutableClassesGiraphConfiguration<>(giraphConfiguration, fragment);
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
