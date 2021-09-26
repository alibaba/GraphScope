package org.apache.giraph.conf;

import static java.util.concurrent.TimeUnit.SECONDS;

import com.alibaba.graphscope.parallel.message.DefaultMessageStoreFactory;
import com.alibaba.graphscope.parallel.message.MessageStoreFactory;
import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.factories.DefaultMessageValueFactory;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.impl.VertexImpl;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.worker.DefaultWorkerContext;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Constants used all over Giraph for configuration.
 */
// CHECKSTYLE: stop InterfaceIsTypeCheck
public interface GiraphConstants {

    /**
     * 1KB in bytes
     */
    int ONE_KB = 1024;
    /**
     * 1MB in bytes
     */
    int ONE_MB = 1024 * 1024;

    //    int clientCacheSize = 128 * ONE_MB;
    int clientCacheSize = 512 * ONE_MB;

    /**
     * VertexOutputFormat class
     */
    ClassConfOption<VertexOutputFormat> VERTEX_OUTPUT_FORMAT_CLASS =
        ClassConfOption.create("giraph.vertexOutputFormatClass", null,
            VertexOutputFormat.class, "VertexOutputFormat class");

    /**
     * VertexOutputFormat class
     */
    ClassConfOption<VertexInputFormat> VERTEX_INPUT_FORMAT_CLASS =
        ClassConfOption.create("giraph.vertexInputFormatClass", null,
            VertexInputFormat.class, "VertexInputFormat class");

    /**
     * EdgeInputFormat class
     */
    ClassConfOption<EdgeInputFormat> EDGE_INPUT_FORMAT_CLASS =
        ClassConfOption.create("giraph.edgeInputFormatClass", null,
            EdgeInputFormat.class, "EdgeInputFormat class");

//    /** EdgeOutputFormat class */
//    ClassConfOption<EdgeOutputFormat> EDGE_OUTPUT_FORMAT_CLASS =
//        ClassConfOption.create("giraph.edgeOutputFormatClass", null,
//            EdgeOutputFormat.class, "EdgeOutputFormat class");
    /**
     * vertexOutputFormat sub-directory In Giraph, the output is to hdfs, they use the parent
     * directory of default parent directory, and use sub directory as sub.
     * <p>
     * In our project, we deem this configuration as ABSOLUTE path.
     */
    StrConfOption VERTEX_OUTPUT_FORMAT_SUBDIR =
        new StrConfOption("giraph.vertex.output.subdir", "",
            "VertexOutputFormat sub-directory");
    StrConfOption VERTEX_OUTPUT_PATH = new StrConfOption("giraph.vertex.output.path", "",
        "vertex output path");

    /**
     * Vertex index class
     */
    ClassConfOption<WritableComparable> VERTEX_ID_CLASS =
        ClassConfOption.create("giraph.vertexIdClass", null,
            WritableComparable.class, "Vertex index class");

    /**
     * Vertex value class
     */
    ClassConfOption<Writable> VERTEX_VALUE_CLASS =
        ClassConfOption.create("giraph.vertexValueClass", null, Writable.class,
            "Vertex value class");

    /**
     * Edge value class
     */
    ClassConfOption<Writable> EDGE_VALUE_CLASS =
        ClassConfOption.create("giraph.edgeValueClass", null, Writable.class,
            "Edge value class");

    /**
     * Vertex class
     */
    ClassConfOption<Vertex> VERTEX_CLASS =
        ClassConfOption.create("giraph.vertexClass",
            VertexImpl.class, Vertex.class,
            "Vertex class");

    /**
     * Outgoing message value class
     */
    ClassConfOption<Writable> OUTGOING_MESSAGE_VALUE_CLASS =
        ClassConfOption.create("giraph.outgoingMessageValueClass", null,
            Writable.class, "Outgoing message value class");

    /**
     * incoming message value class
     */
    ClassConfOption<Writable> INCOMING_MESSAGE_VALUE_CLASS =
        ClassConfOption.create("giraph.incomingMessageValueClass", null,
            Writable.class, "Outgoing message value class");

    /**
     * Worker context class
     */
    ClassConfOption<WorkerContext> WORKER_CONTEXT_CLASS =
        ClassConfOption.create("giraph.workerContextClass",
            DefaultWorkerContext.class, WorkerContext.class,
            "Worker contextclass");

    /**
     * Worker context class
     */
    ClassConfOption<AbstractComputation> COMPUTATION_CLASS =
        ClassConfOption.create("giraph.computationClass",
            null, AbstractComputation.class,
            "User computation class");

    /**
     * TypesHolder, used if Computation not set - optional
     */
    ClassConfOption<TypesHolder> TYPES_HOLDER_CLASS =
        ClassConfOption.create("giraph.typesHolder", null,
            TypesHolder.class,
            "TypesHolder, used if Computation not set - optional");

    /**
     * Message combiner class - optional
     */
    ClassConfOption<MessageCombiner> MESSAGE_COMBINER_CLASS =
        ClassConfOption.create("giraph.messageCombinerClass", null,
            MessageCombiner.class, "Message combiner class - optional");

    /**
     * Outgoing message value factory class - optional
     */
    ClassConfOption<MessageValueFactory>
        OUTGOING_MESSAGE_VALUE_FACTORY_CLASS =
        ClassConfOption.create("giraph.outgoingMessageValueFactoryClass",
            DefaultMessageValueFactory.class, MessageValueFactory.class,
            "Outgoing message value factory class - optional");

    /**
     * Class for Master - optional
     */
    ClassConfOption<MasterCompute> MASTER_COMPUTE_CLASS =
        ClassConfOption.create("giraph.masterComputeClass",
            DefaultMasterCompute.class, MasterCompute.class,
            "Class for Master - optional");

    /**
     * Number of channels used per server
     */
    IntConfOption CHANNELS_PER_SERVER =
        new IntConfOption("giraph.channelsPerServer", 1,
            "Number of channels used per server");


    /**
     * Warn if msg request size exceeds default size by this factor
     */
    FloatConfOption REQUEST_SIZE_WARNING_THRESHOLD = new FloatConfOption(
        "giraph.msgRequestWarningThreshold", 2.0f,
        "If request sizes are bigger than the buffer size by this factor " +
            "warnings are printed to the log and to the command line");
    /**
     * TCP backlog (defaults to number of workers)
     */
    IntConfOption TCP_BACKLOG = new IntConfOption("giraph.tcpBacklog", 1,
        "TCP backlog (defaults to number of workers)");

    /**
     * Use netty pooled memory buffer allocator
     */
    BooleanConfOption NETTY_USE_POOLED_ALLOCATOR = new BooleanConfOption(
        "giraph.useNettyPooledAllocator", true, "Should netty use pooled " +
        "memory allocator?");

    /**
     * Use direct memory buffers in netty
     */
    BooleanConfOption NETTY_USE_DIRECT_MEMORY = new BooleanConfOption(
        "giraph.useNettyDirectMemory", true, "Should netty use direct " +
        "memory buffers");

    /**
     * How big to make the encoder buffer?
     */
    IntConfOption NETTY_REQUEST_ENCODER_BUFFER_SIZE =
        new IntConfOption("giraph.nettyRequestEncoderBufferSize", clientCacheSize,
            "How big to make the encoder buffer?");
    /**
     * Client send buffer size
     */
    IntConfOption CLIENT_SEND_BUFFER_SIZE =
        new IntConfOption("giraph.clientSendBufferSize", clientCacheSize,
            "Client send buffer size");

    /**
     * Client receive buffer size
     */
    IntConfOption CLIENT_RECEIVE_BUFFER_SIZE =
        new IntConfOption("giraph.clientReceiveBufferSize", 32 * ONE_KB,
            "Client receive buffer size");

    /**
     * Server send buffer size
     */
    IntConfOption SERVER_SEND_BUFFER_SIZE =
        new IntConfOption("giraph.serverSendBufferSize", 32 * ONE_KB,
            "Server send buffer size");

    /**
     * Server receive buffer size. a little bit larger than request size.
     */
    IntConfOption SERVER_RECEIVE_BUFFER_SIZE =
        new IntConfOption("giraph.serverReceiveBufferSize", clientCacheSize,
            "Server receive buffer size");

    IntConfOption MAX_FRAME_LENGTH = new IntConfOption("giraph.maxFrameLength", clientCacheSize,
        "fixed frame max size");

    /**
     * This represents the size of message cache, so actual cache bytes is generally (8 + 8) *
     * aggregate_size; before flush.
     * <p>
     * This is only used by batchWritable cache.
     */
    IntConfOption MESSAGE_AGGREGATE_SIZE = new IntConfOption("giraph.messageAggregateSize",
        64 * ONE_KB,
        "how many size of request we aggregate together for sending in bulk");

    /**
     * Should be used by byteBuf cache.
     */
    IntConfOption MAX_OUT_MSG_CACHE_SIZE = new IntConfOption("giraph.maxOutMsgCacheSize",
        clientCacheSize,
        "Max number of bytes in cache before flushing");

    /**
     * Netty client threads
     */
    IntConfOption NETTY_CLIENT_THREADS =
        new IntConfOption("giraph.nettyClientThreads", 4, "Netty client threads");

    /**
     * Netty server boss threads
     */
    IntConfOption NETTY_SERVER_BOSS_THREADS =
        new IntConfOption("giraph.nettyServerBossThreads", 4,
            "Netty server boss threads");
    /**
     * Netty server worker threads
     */
    IntConfOption NETTY_SERVER_WORKER_THREADS =
        new IntConfOption("giraph.nettyServerWorkerThreads", 16,
            "Netty server worker threads");

    /**
     * Use the execution handler in netty on the client?
     */
    BooleanConfOption NETTY_CLIENT_USE_EXECUTION_HANDLER =
        new BooleanConfOption("giraph.nettyClientUseExecutionHandler", true,
            "Use the execution handler in netty on the client?");

    /**
     * Netty client execution threads (execution handler)
     */
    IntConfOption NETTY_CLIENT_EXECUTION_THREADS =
        new IntConfOption("giraph.nettyClientExecutionThreads", 8,
            "Netty client execution threads (execution handler)");

    /**
     * Where to place the netty client execution handle?
     */
    StrConfOption NETTY_CLIENT_EXECUTION_AFTER_HANDLER =
        new StrConfOption("giraph.nettyClientExecutionAfterHandler",
            "request-encoder",
            "Where to place the netty client execution handle?");

    /**
     * Use the execution handler in netty on the server?
     */
    BooleanConfOption NETTY_SERVER_USE_EXECUTION_HANDLER =
        new BooleanConfOption("giraph.nettyServerUseExecutionHandler", true,
            "Use the execution handler in netty on the server?");

    /**
     * Netty server execution threads (execution handler)
     */
    IntConfOption NETTY_SERVER_EXECUTION_THREADS =
        new IntConfOption("giraph.nettyServerExecutionThreads", 8,
            "Netty server execution threads (execution handler)");

    /**
     * Where to place the netty server execution handle?
     */
    StrConfOption NETTY_SERVER_EXECUTION_AFTER_HANDLER =
        new StrConfOption("giraph.nettyServerExecutionAfterHandler",
            "requestFrameDecoder",
            "Where to place the netty server execution handle?");

    /**
     * Netty simulate a first request closed
     */
    BooleanConfOption NETTY_SIMULATE_FIRST_REQUEST_CLOSED =
        new BooleanConfOption("giraph.nettySimulateFirstRequestClosed", false,
            "Netty simulate a first request closed");

    /**
     * Netty simulate a first response failed
     */
    BooleanConfOption NETTY_SIMULATE_FIRST_RESPONSE_FAILED =
        new BooleanConfOption("giraph.nettySimulateFirstResponseFailed", false,
            "Netty simulate a first response failed");

    /**
     * Netty - set which compression to use
     */
    StrConfOption NETTY_COMPRESSION_ALGORITHM =
        new StrConfOption("giraph.nettyCompressionAlgorithm", "",
            "Which compression algorithm to use in netty");
    /**
     * Msecs to wait between waiting for all requests to finish
     */
    IntConfOption WAITING_REQUEST_MSECS =
        new IntConfOption("giraph.waitingRequestMsecs", SECONDS.toMillis(15),
            "Msecs to wait between waiting for all requests to finish");

    /**
     * Maximum number of simultaneous worker tasks started by this job (int).
     */
    String MAX_WORKERS = "giraph.maxWorkers";
    /**
     * Initial port to start using for the IPC communication
     */
    IntConfOption IPC_INITIAL_PORT =
        new IntConfOption("giraph.ipcInitialPort", 30000,
            "Initial port to start using for the IPC communication");

    /**
     * Maximum bind attempts for different IPC ports
     */
    IntConfOption MAX_IPC_PORT_BIND_ATTEMPTS =
        new IntConfOption("giraph.maxIpcPortBindAttempts", 20,
            "Maximum bind attempts for different IPC ports");
    /**
     * Maximum connections trys for client to connect to server
     */
    IntConfOption MAX_CONN_TRY_ATTEMPTS = new IntConfOption("giraph.maxConnTryAttempty", 20,
        "Maximum bind attempts for client to connect to server");

    IntConfOption MESSAGE_MANAGER_BASE_SERVER_PORT = new IntConfOption("giraph.mmBaseServerPort",
        30000,
        "The base port for messager to communicate");

    IntConfOption AGGREGATOR_BASE_SERVER_PORT = new IntConfOption("giraph.aggregatorBaseServerPort",
        40000,
        "The base port for aggregator to communicate");

//    StrConfOption MESSAGE_MANAGER_TYPE = new StrConfOption("giraph.messageManagerType", "netty",
//        "default message manager to use");

    StrConfOption MESSAGE_MANAGER_TYPE = new StrConfOption("giraph.messageManagerType", "mpi",
        "default message manager to use");

    IntConfOption INET_ADDRESS_MAX_RESOLVE_TIMES = new IntConfOption(
        "giraph.inetAddressMaxResolveTimes", 10,
        "max tries for verifying address for client.");

    /**
     * Use unsafe serialization?
     */
    BooleanConfOption USE_UNSAFE_SERIALIZATION =
        new BooleanConfOption("giraph.useUnsafeSerialization", true,
            "Use unsafe serialization?");

    /**
     * Message Store Factory
     */
    ClassConfOption<MessageStoreFactory> MESSAGE_STORE_FACTORY_CLASS =
        ClassConfOption.create("giraph.messageStoreFactoryClass",
            DefaultMessageStoreFactory.class,
            MessageStoreFactory.class,
            "Message Store Factory Class that is to be used");


    /**
     * Default use byteBuf message cache.
     */
    StrConfOption OUT_MESSAGE_CACHE_TYPE = new StrConfOption("giraph.outMessageCacheType",
        "ByteBuf", "which type of out message cache to use");

    StrConfOption EDGE_MANAGER = new StrConfOption("girpah.edgeManager", "eager", "eager or laze");
}
