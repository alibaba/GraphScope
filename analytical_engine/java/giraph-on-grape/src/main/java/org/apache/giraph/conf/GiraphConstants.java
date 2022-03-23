/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *   	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.giraph.conf;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.alibaba.graphscope.graph.impl.VertexImpl;
import com.alibaba.graphscope.parallel.message.DefaultMessageStoreFactory;
import com.alibaba.graphscope.parallel.message.MessageStoreFactory;

import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.aggregators.TextAggregatorWriter;
import org.apache.giraph.bsp.BspOutputFormat;
import org.apache.giraph.bsp.checkpoints.CheckpointSupportedChecker;
import org.apache.giraph.bsp.checkpoints.DefaultCheckpointSupportedChecker;
import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.comm.messages.MessageEncodeAndStoreType;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.edge.CreateSourceVertexCallback;
import org.apache.giraph.edge.DefaultCreateSourceVertexCallback;
import org.apache.giraph.edge.EdgeStoreFactory;
import org.apache.giraph.edge.InMemoryEdgeStoreFactory;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.factories.ComputationFactory;
import org.apache.giraph.factories.DefaultComputationFactory;
import org.apache.giraph.factories.DefaultEdgeValueFactory;
import org.apache.giraph.factories.DefaultInputOutEdgesFactory;
import org.apache.giraph.factories.DefaultMessageValueFactory;
import org.apache.giraph.factories.DefaultOutEdgesFactory;
import org.apache.giraph.factories.DefaultVertexIdFactory;
import org.apache.giraph.factories.DefaultVertexValueFactory;
import org.apache.giraph.factories.EdgeValueFactory;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.factories.OutEdgesFactory;
import org.apache.giraph.factories.VertexIdFactory;
import org.apache.giraph.factories.VertexValueFactory;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.DefaultVertexResolver;
import org.apache.giraph.graph.DefaultVertexValueCombiner;
import org.apache.giraph.graph.JobProgressTrackerClient;
import org.apache.giraph.graph.Language;
import org.apache.giraph.graph.MapperObserver;
import org.apache.giraph.graph.RetryableJobProgressTrackerClient;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexResolver;
import org.apache.giraph.graph.VertexValueCombiner;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.EdgeOutputFormat;
import org.apache.giraph.io.MappingInputFormat;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.filters.DefaultEdgeInputFilter;
import org.apache.giraph.io.filters.DefaultVertexInputFilter;
import org.apache.giraph.io.filters.EdgeInputFilter;
import org.apache.giraph.io.filters.VertexInputFilter;
import org.apache.giraph.job.DefaultGiraphJobRetryChecker;
import org.apache.giraph.job.DefaultJobObserver;
import org.apache.giraph.job.DefaultJobProgressTrackerService;
import org.apache.giraph.job.GiraphJobObserver;
import org.apache.giraph.job.GiraphJobRetryChecker;
import org.apache.giraph.job.HaltApplicationUtils;
import org.apache.giraph.job.JobProgressTrackerService;
import org.apache.giraph.mapping.MappingStore;
import org.apache.giraph.mapping.MappingStoreOps;
import org.apache.giraph.mapping.translate.TranslateEdge;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.master.MasterObserver;
import org.apache.giraph.ooc.persistence.LocalDiskDataAccessor;
import org.apache.giraph.ooc.persistence.OutOfCoreDataAccessor;
import org.apache.giraph.ooc.policy.MemoryEstimatorOracle;
import org.apache.giraph.ooc.policy.OutOfCoreOracle;
import org.apache.giraph.partition.GraphPartitionerFactory;
import org.apache.giraph.partition.HashPartitionerFactory;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.partition.SimplePartition;
import org.apache.giraph.utils.GcObserver;
import org.apache.giraph.worker.DefaultWorkerContext;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerObserver;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.OutputFormat;

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
     * How big to make the encoder buffer?
     */
    IntConfOption NETTY_REQUEST_ENCODER_BUFFER_SIZE =
            new IntConfOption(
                    "giraph.nettyRequestEncoderBufferSize",
                    clientCacheSize,
                    "How big to make the encoder buffer?");
    /**
     * Client send buffer size
     */
    IntConfOption CLIENT_SEND_BUFFER_SIZE =
            new IntConfOption(
                    "giraph.clientSendBufferSize", clientCacheSize, "Client send buffer size");
    /**
     * Server receive buffer size. a little bit larger than request size.
     */
    IntConfOption SERVER_RECEIVE_BUFFER_SIZE =
            new IntConfOption(
                    "giraph.serverReceiveBufferSize",
                    clientCacheSize,
                    "Server receive buffer size");

    //    /** EdgeOutputFormat class */
    //    ClassConfOption<EdgeOutputFormat> EDGE_OUTPUT_FORMAT_CLASS =
    //        ClassConfOption.create("giraph.edgeOutputFormatClass", null,
    //            EdgeOutputFormat.class, "EdgeOutputFormat class");
    IntConfOption MAX_FRAME_LENGTH =
            new IntConfOption("giraph.maxFrameLength", clientCacheSize, "fixed frame max size");
    /**
     * Should be used by byteBuf cache.
     */
    IntConfOption MAX_OUT_MSG_CACHE_SIZE =
            new IntConfOption(
                    "giraph.maxOutMsgCacheSize",
                    clientCacheSize,
                    "Max number of bytes in cache before flushing");
    /**
     * VertexOutputFormat class
     */
    ClassConfOption<VertexOutputFormat> VERTEX_OUTPUT_FORMAT_CLASS =
            ClassConfOption.create(
                    "giraph.vertexOutputFormatClass",
                    null,
                    VertexOutputFormat.class,
                    "VertexOutputFormat class");
    /**
     * VertexOutputFormat class
     */
    ClassConfOption<VertexInputFormat> VERTEX_INPUT_FORMAT_CLASS =
            ClassConfOption.create(
                    "giraph.vertexInputFormatClass",
                    null,
                    VertexInputFormat.class,
                    "VertexInputFormat class");
    /**
     * EdgeInputFormat class
     */
    ClassConfOption<EdgeInputFormat> EDGE_INPUT_FORMAT_CLASS =
            ClassConfOption.create(
                    "giraph.edgeInputFormatClass",
                    null,
                    EdgeInputFormat.class,
                    "EdgeInputFormat class");
    /**
     * vertexOutputFormat sub-directory In Giraph, the output is to hdfs, they use the parent
     * directory of default parent directory, and use sub directory as sub.
     * <p>
     * In our project, we deem this configuration as ABSOLUTE path.
     */
    StrConfOption VERTEX_OUTPUT_FORMAT_SUBDIR =
            new StrConfOption(
                    "giraph.vertex.output.subdir", "", "VertexOutputFormat sub-directory");

    StrConfOption VERTEX_OUTPUT_PATH =
            new StrConfOption("giraph.vertex.output.path", "", "vertex output path");
    /**
     * Vertex index class
     */
    ClassConfOption<WritableComparable> VERTEX_ID_CLASS =
            ClassConfOption.create(
                    "giraph.vertexIdClass", null, WritableComparable.class, "Vertex index class");
    /**
     * Vertex value class
     */
    ClassConfOption<Writable> VERTEX_VALUE_CLASS =
            ClassConfOption.create(
                    "giraph.vertexValueClass", null, Writable.class, "Vertex value class");
    /**
     * Edge value class
     */
    ClassConfOption<Writable> EDGE_VALUE_CLASS =
            ClassConfOption.create(
                    "giraph.edgeValueClass", null, Writable.class, "Edge value class");
    /**
     * Vertex class
     */
    ClassConfOption<Vertex> VERTEX_CLASS =
            ClassConfOption.create(
                    "giraph.vertexClass", VertexImpl.class, Vertex.class, "Vertex class");
    /**
     * Outgoing message value class
     */
    ClassConfOption<Writable> OUTGOING_MESSAGE_VALUE_CLASS =
            ClassConfOption.create(
                    "giraph.outgoingMessageValueClass",
                    null,
                    Writable.class,
                    "Outgoing message value class");
    /**
     * incoming message value class
     */
    ClassConfOption<Writable> INCOMING_MESSAGE_VALUE_CLASS =
            ClassConfOption.create(
                    "giraph.incomingMessageValueClass",
                    null,
                    Writable.class,
                    "Outgoing message value class");
    /**
     * Worker context class
     */
    ClassConfOption<WorkerContext> WORKER_CONTEXT_CLASS =
            ClassConfOption.create(
                    "giraph.workerContextClass",
                    DefaultWorkerContext.class,
                    WorkerContext.class,
                    "Worker contextclass");
    /**
     * Worker context class
     */
    ClassConfOption<AbstractComputation> COMPUTATION_CLASS =
            ClassConfOption.create(
                    "giraph.computationClass",
                    null,
                    AbstractComputation.class,
                    "User computation class");
    /**
     * TypesHolder, used if Computation not set - optional
     */
    ClassConfOption<TypesHolder> TYPES_HOLDER_CLASS =
            ClassConfOption.create(
                    "giraph.typesHolder",
                    null,
                    TypesHolder.class,
                    "TypesHolder, used if Computation not set - optional");
    /**
     * Message combiner class - optional
     */
    ClassConfOption<MessageCombiner> MESSAGE_COMBINER_CLASS =
            ClassConfOption.create(
                    "giraph.messageCombinerClass",
                    null,
                    MessageCombiner.class,
                    "Message combiner class - optional");
    /**
     * Outgoing message value factory class - optional
     */
    ClassConfOption<MessageValueFactory> OUTGOING_MESSAGE_VALUE_FACTORY_CLASS =
            ClassConfOption.create(
                    "giraph.outgoingMessageValueFactoryClass",
                    DefaultMessageValueFactory.class,
                    MessageValueFactory.class,
                    "Outgoing message value factory class - optional");
    /**
     * Class for Master - optional
     */
    ClassConfOption<MasterCompute> MASTER_COMPUTE_CLASS =
            ClassConfOption.create(
                    "giraph.masterComputeClass",
                    DefaultMasterCompute.class,
                    MasterCompute.class,
                    "Class for Master - optional");
    /**
     * Number of channels used per server
     */
    IntConfOption CHANNELS_PER_SERVER =
            new IntConfOption("giraph.channelsPerServer", 1, "Number of channels used per server");
    /**
     * Warn if msg request size exceeds default size by this factor
     */
    FloatConfOption REQUEST_SIZE_WARNING_THRESHOLD =
            new FloatConfOption(
                    "giraph.msgRequestWarningThreshold",
                    2.0f,
                    "If request sizes are bigger than the buffer size by this factor "
                            + "warnings are printed to the log and to the command line");
    /**
     * TCP backlog (defaults to number of workers)
     */
    IntConfOption TCP_BACKLOG =
            new IntConfOption(
                    "giraph.tcpBacklog", 1, "TCP backlog (defaults to number of workers)");
    /**
     * Use netty pooled memory buffer allocator
     */
    BooleanConfOption NETTY_USE_POOLED_ALLOCATOR =
            new BooleanConfOption(
                    "giraph.useNettyPooledAllocator",
                    true,
                    "Should netty use pooled " + "memory allocator?");
    /**
     * Use direct memory buffers in netty
     */
    BooleanConfOption NETTY_USE_DIRECT_MEMORY =
            new BooleanConfOption(
                    "giraph.useNettyDirectMemory",
                    true,
                    "Should netty use direct " + "memory buffers");
    /**
     * Client receive buffer size
     */
    IntConfOption CLIENT_RECEIVE_BUFFER_SIZE =
            new IntConfOption(
                    "giraph.clientReceiveBufferSize", 32 * ONE_KB, "Client receive buffer size");
    /**
     * Server send buffer size
     */
    IntConfOption SERVER_SEND_BUFFER_SIZE =
            new IntConfOption(
                    "giraph.serverSendBufferSize", 32 * ONE_KB, "Server send buffer size");
    /**
     * This represents the size of message cache, so actual cache bytes is generally (8 + 8) *
     * aggregate_size; before flush.
     * <p>
     * This is only used by batchWritable cache.
     */
    IntConfOption MESSAGE_AGGREGATE_SIZE =
            new IntConfOption(
                    "giraph.messageAggregateSize",
                    64 * ONE_KB,
                    "how many size of request we aggregate together for sending in bulk");
    /**
     * Netty client threads
     */
    IntConfOption NETTY_CLIENT_THREADS =
            new IntConfOption("giraph.nettyClientThreads", 4, "Netty client threads");

    /**
     * Netty server boss threads
     */
    IntConfOption NETTY_SERVER_BOSS_THREADS =
            new IntConfOption("giraph.nettyServerBossThreads", 4, "Netty server boss threads");
    /**
     * Netty server worker threads
     */
    IntConfOption NETTY_SERVER_WORKER_THREADS =
            new IntConfOption("giraph.nettyServerWorkerThreads", 16, "Netty server worker threads");

    /**
     * Use the execution handler in netty on the client?
     */
    BooleanConfOption NETTY_CLIENT_USE_EXECUTION_HANDLER =
            new BooleanConfOption(
                    "giraph.nettyClientUseExecutionHandler",
                    true,
                    "Use the execution handler in netty on the client?");

    /**
     * Netty client execution threads (execution handler)
     */
    IntConfOption NETTY_CLIENT_EXECUTION_THREADS =
            new IntConfOption(
                    "giraph.nettyClientExecutionThreads",
                    8,
                    "Netty client execution threads (execution handler)");

    /**
     * Where to place the netty client execution handle?
     */
    StrConfOption NETTY_CLIENT_EXECUTION_AFTER_HANDLER =
            new StrConfOption(
                    "giraph.nettyClientExecutionAfterHandler",
                    "request-encoder",
                    "Where to place the netty client execution handle?");

    /**
     * Use the execution handler in netty on the server?
     */
    BooleanConfOption NETTY_SERVER_USE_EXECUTION_HANDLER =
            new BooleanConfOption(
                    "giraph.nettyServerUseExecutionHandler",
                    true,
                    "Use the execution handler in netty on the server?");

    /**
     * Netty server execution threads (execution handler)
     */
    IntConfOption NETTY_SERVER_EXECUTION_THREADS =
            new IntConfOption(
                    "giraph.nettyServerExecutionThreads",
                    8,
                    "Netty server execution threads (execution handler)");

    /**
     * Where to place the netty server execution handle?
     */
    StrConfOption NETTY_SERVER_EXECUTION_AFTER_HANDLER =
            new StrConfOption(
                    "giraph.nettyServerExecutionAfterHandler",
                    "requestFrameDecoder",
                    "Where to place the netty server execution handle?");

    /**
     * Netty simulate a first request closed
     */
    BooleanConfOption NETTY_SIMULATE_FIRST_REQUEST_CLOSED =
            new BooleanConfOption(
                    "giraph.nettySimulateFirstRequestClosed",
                    false,
                    "Netty simulate a first request closed");

    /**
     * Netty simulate a first response failed
     */
    BooleanConfOption NETTY_SIMULATE_FIRST_RESPONSE_FAILED =
            new BooleanConfOption(
                    "giraph.nettySimulateFirstResponseFailed",
                    false,
                    "Netty simulate a first response failed");

    /**
     * Netty - set which compression to use
     */
    StrConfOption NETTY_COMPRESSION_ALGORITHM =
            new StrConfOption(
                    "giraph.nettyCompressionAlgorithm",
                    "",
                    "Which compression algorithm to use in netty");
    /**
     * Msecs to wait between waiting for all requests to finish
     */
    IntConfOption WAITING_REQUEST_MSECS =
            new IntConfOption(
                    "giraph.waitingRequestMsecs",
                    SECONDS.toMillis(15),
                    "Msecs to wait between waiting for all requests to finish");

    /**
     * Maximum number of simultaneous worker tasks started by this job (int).
     */
    String MAX_WORKERS = "giraph.maxWorkers";
    /**
     * Initial port to start using for the IPC communication
     */
    IntConfOption IPC_INITIAL_PORT =
            new IntConfOption(
                    "giraph.ipcInitialPort",
                    30000,
                    "Initial port to start using for the IPC communication");

    /**
     * Maximum bind attempts for different IPC ports
     */
    IntConfOption MAX_IPC_PORT_BIND_ATTEMPTS =
            new IntConfOption(
                    "giraph.maxIpcPortBindAttempts",
                    20,
                    "Maximum bind attempts for different IPC ports");
    /**
     * Maximum connections trys for client to connect to server
     */
    IntConfOption MAX_CONN_TRY_ATTEMPTS =
            new IntConfOption(
                    "giraph.maxConnTryAttempty",
                    20,
                    "Maximum bind attempts for client to connect to server");

    IntConfOption MESSAGE_MANAGER_BASE_SERVER_PORT =
            new IntConfOption(
                    "giraph.mmBaseServerPort", 30000, "The base port for messager to communicate");

    IntConfOption AGGREGATOR_BASE_SERVER_PORT =
            new IntConfOption(
                    "giraph.aggregatorBaseServerPort",
                    40000,
                    "The base port for aggregator to communicate");

    //    StrConfOption MESSAGE_MANAGER_TYPE = new StrConfOption("giraph.messageManagerType",
    // "netty",
    //        "default message manager to use");

    StrConfOption MESSAGE_MANAGER_TYPE =
            new StrConfOption("giraph.messageManagerType", "mpi", "default message manager to use");

    IntConfOption INET_ADDRESS_MAX_RESOLVE_TIMES =
            new IntConfOption(
                    "giraph.inetAddressMaxResolveTimes",
                    10,
                    "max tries for verifying address for client.");

    /**
     * Use unsafe serialization?
     */
    BooleanConfOption USE_UNSAFE_SERIALIZATION =
            new BooleanConfOption(
                    "giraph.useUnsafeSerialization", true, "Use unsafe serialization?");

    /**
     * Message Store Factory
     */
    ClassConfOption<MessageStoreFactory> MESSAGE_STORE_FACTORY_CLASS =
            ClassConfOption.create(
                    "giraph.messageStoreFactoryClass",
                    DefaultMessageStoreFactory.class,
                    MessageStoreFactory.class,
                    "Message Store Factory Class that is to be used");

    BooleanConfOption USE_PRIMITIVE_MESSAGE_STORE =
            new BooleanConfOption(
                    "giraph.userPrimitiveMessageStore",
                    false,
                    "user specialized primitive store or not");

    /**
     * Default use byteBuf message cache.
     */
    StrConfOption OUT_MESSAGE_CACHE_TYPE =
            new StrConfOption(
                    "giraph.outMessageCacheType",
                    "ByteBuf",
                    "which type of out message cache to use");

    StrConfOption EDGE_MANAGER =
            new StrConfOption("girpah.edgeManager", "default", "default or lazy");

    ClassConfOption<ComputationFactory> COMPUTATION_FACTORY_CLASS =
            ClassConfOption.create(
                    "giraph.computation.factory.class",
                    DefaultComputationFactory.class,
                    ComputationFactory.class,
                    "Computation factory class - optional");

    /**
     * Vertex edges class - optional
     */
    ClassConfOption<OutEdges> VERTEX_EDGES_CLASS =
            ClassConfOption.create(
                    "giraph.outEdgesClass",
                    ByteArrayEdges.class,
                    OutEdges.class,
                    "Vertex edges class - optional");

    ClassConfOption<OutEdges> INPUT_VERTEX_EDGES_CLASS =
            ClassConfOption.create(
                    "giraph.inputOutEdgesClass",
                    ByteArrayEdges.class,
                    OutEdges.class,
                    "Vertex edges class to be used during edge input only - optional");
    /**
     * Graph partitioner factory class - optional
     */
    ClassConfOption<GraphPartitionerFactory> GRAPH_PARTITIONER_FACTORY_CLASS =
            ClassConfOption.create(
                    "giraph.graphPartitionerFactoryClass",
                    HashPartitionerFactory.class,
                    GraphPartitionerFactory.class,
                    "Graph partitioner factory class - optional");

    ClassConfOption<EdgeOutputFormat> EDGE_OUTPUT_FORMAT_CLASS =
            ClassConfOption.create(
                    "giraph.edgeOutputFormatClass",
                    null,
                    EdgeOutputFormat.class,
                    "EdgeOutputFormat class");

    /**
     * MappingInputFormat class
     */
    ClassConfOption<MappingInputFormat> MAPPING_INPUT_FORMAT_CLASS =
            ClassConfOption.create(
                    "giraph.mappingInputFormatClass",
                    null,
                    MappingInputFormat.class,
                    "MappingInputFormat class");

    ClassConfOption<AggregatorWriter> AGGREGATOR_WRITER_CLASS =
            ClassConfOption.create(
                    "giraph.aggregatorWriterClass",
                    TextAggregatorWriter.class,
                    AggregatorWriter.class,
                    "AggregatorWriter class - optional");
    EnumConfOption<MessageEncodeAndStoreType> MESSAGE_ENCODE_AND_STORE_TYPE =
            EnumConfOption.create(
                    "giraph.messageEncodeAndStoreType",
                    MessageEncodeAndStoreType.class,
                    MessageEncodeAndStoreType.BYTEARRAY_PER_PARTITION,
                    "Select the message_encode_and_store_type to use");
    ClassConfOption<VertexResolver> VERTEX_RESOLVER_CLASS =
            ClassConfOption.create(
                    "giraph.vertexResolverClass",
                    DefaultVertexResolver.class,
                    VertexResolver.class,
                    "Vertex resolver class - optional");
    ClassConfOption<VertexValueCombiner> VERTEX_VALUE_COMBINER_CLASS =
            ClassConfOption.create(
                    "giraph.vertexValueCombinerClass",
                    DefaultVertexValueCombiner.class,
                    VertexValueCombiner.class,
                    "Vertex value combiner class - optional");
    ClassConfOption<Partition> PARTITION_CLASS =
            ClassConfOption.create(
                    "giraph.partitionClass",
                    SimplePartition.class,
                    Partition.class,
                    "Partition class - optional");
    /**
     * EdgeInputFilter class
     */
    ClassConfOption<EdgeInputFilter> EDGE_INPUT_FILTER_CLASS =
            ClassConfOption.create(
                    "giraph.edgeInputFilterClass",
                    DefaultEdgeInputFilter.class,
                    EdgeInputFilter.class,
                    "EdgeInputFilter class");
    /**
     * VertexInputFilter class
     */
    ClassConfOption<VertexInputFilter> VERTEX_INPUT_FILTER_CLASS =
            ClassConfOption.create(
                    "giraph.vertexInputFilterClass",
                    DefaultVertexInputFilter.class,
                    VertexInputFilter.class,
                    "VertexInputFilter class");

    /**
     * Mapping related information
     */
    ClassConfOption<MappingStore> MAPPING_STORE_CLASS =
            ClassConfOption.create(
                    "giraph.mappingStoreClass", null, MappingStore.class, "MappingStore Class");

    /**
     * Class to use for performing read operations on mapping store
     */
    ClassConfOption<MappingStoreOps> MAPPING_STORE_OPS_CLASS =
            ClassConfOption.create(
                    "giraph.mappingStoreOpsClass",
                    null,
                    MappingStoreOps.class,
                    "MappingStoreOps class");

    /**
     * Upper value of LongByteMappingStore
     */
    IntConfOption LB_MAPPINGSTORE_UPPER =
            new IntConfOption(
                    "giraph.lbMappingStoreUpper", -1, "'upper' value used by lbmappingstore");
    /**
     * Lower value of LongByteMappingStore
     */
    IntConfOption LB_MAPPINGSTORE_LOWER =
            new IntConfOption(
                    "giraph.lbMappingStoreLower", -1, "'lower' value used by lbMappingstore");
    /**
     * Class used to conduct expensive edge translation during vertex input
     */
    ClassConfOption EDGE_TRANSLATION_CLASS =
            ClassConfOption.create(
                    "giraph.edgeTranslationClass",
                    null,
                    TranslateEdge.class,
                    "Class used to conduct expensive edge "
                            + "translation during vertex input phase");

    /**
     * Edge Store Factory
     */
    ClassConfOption<EdgeStoreFactory> EDGE_STORE_FACTORY_CLASS =
            ClassConfOption.create(
                    "giraph.edgeStoreFactoryClass",
                    InMemoryEdgeStoreFactory.class,
                    EdgeStoreFactory.class,
                    "Edge Store Factory class to use for creating edgeStore");

    /**
     * Language user's graph types are implemented in
     */
    PerGraphTypeEnumConfOption<Language> GRAPH_TYPE_LANGUAGES =
            PerGraphTypeEnumConfOption.create(
                    "giraph.types.language",
                    Language.class,
                    Language.JAVA,
                    "Language user graph types (IVEMM) are implemented in");

    /**
     * Whether user graph types need wrappers
     */
    PerGraphTypeBooleanConfOption GRAPH_TYPES_NEEDS_WRAPPERS =
            new PerGraphTypeBooleanConfOption(
                    "giraph.jython.type.wrappers",
                    false,
                    "Whether user graph types (IVEMM) need Jython wrappers");

    /**
     * Vertex id factory class - optional
     */
    ClassConfOption<VertexIdFactory> VERTEX_ID_FACTORY_CLASS =
            ClassConfOption.create(
                    "giraph.vertexIdFactoryClass",
                    DefaultVertexIdFactory.class,
                    VertexIdFactory.class,
                    "Vertex ID factory class - optional");
    /**
     * Vertex value factory class - optional
     */
    ClassConfOption<VertexValueFactory> VERTEX_VALUE_FACTORY_CLASS =
            ClassConfOption.create(
                    "giraph.vertexValueFactoryClass",
                    DefaultVertexValueFactory.class,
                    VertexValueFactory.class,
                    "Vertex value factory class - optional");
    /**
     * Edge value factory class - optional
     */
    ClassConfOption<EdgeValueFactory> EDGE_VALUE_FACTORY_CLASS =
            ClassConfOption.create(
                    "giraph.edgeValueFactoryClass",
                    DefaultEdgeValueFactory.class,
                    EdgeValueFactory.class,
                    "Edge value factory class - optional");

    /**
     * OutEdges factory class - optional
     */
    ClassConfOption<OutEdgesFactory> VERTEX_EDGES_FACTORY_CLASS =
            ClassConfOption.create(
                    "giraph.outEdgesFactoryClass",
                    DefaultOutEdgesFactory.class,
                    OutEdgesFactory.class,
                    "OutEdges factory class - optional");
    /**
     * OutEdges for input factory class - optional
     */
    ClassConfOption<OutEdgesFactory> INPUT_VERTEX_EDGES_FACTORY_CLASS =
            ClassConfOption.create(
                    "giraph.inputOutEdgesFactoryClass",
                    DefaultInputOutEdgesFactory.class,
                    OutEdgesFactory.class,
                    "OutEdges for input factory class - optional");

    /**
     * Classes for Master Observer - optional
     */
    ClassConfOption<MasterObserver> MASTER_OBSERVER_CLASSES =
            ClassConfOption.create(
                    "giraph.master.observers",
                    null,
                    MasterObserver.class,
                    "Classes for Master Observer - optional");
    /**
     * Classes for Worker Observer - optional
     */
    ClassConfOption<WorkerObserver> WORKER_OBSERVER_CLASSES =
            ClassConfOption.create(
                    "giraph.worker.observers",
                    null,
                    WorkerObserver.class,
                    "Classes for Worker Observer - optional");
    /**
     * Classes for Mapper Observer - optional
     */
    ClassConfOption<MapperObserver> MAPPER_OBSERVER_CLASSES =
            ClassConfOption.create(
                    "giraph.mapper.observers",
                    null,
                    MapperObserver.class,
                    "Classes for Mapper Observer - optional");
    /**
     * Classes for GC Observer - optional
     */
    ClassConfOption<GcObserver> GC_OBSERVER_CLASSES =
            ClassConfOption.create(
                    "giraph.gc.observers",
                    null,
                    GcObserver.class,
                    "Classes for GC oObserver - optional");

    /**
     * Which language computation is implemented in
     */
    EnumConfOption<Language> COMPUTATION_LANGUAGE =
            EnumConfOption.create(
                    "giraph.computation.language",
                    Language.class,
                    Language.JAVA,
                    "Which language computation is implemented in");

    /**
     * Option of whether to create vertexes that were not existent before but received messages
     */
    BooleanConfOption RESOLVER_CREATE_VERTEX_ON_MSGS =
            new BooleanConfOption(
                    "giraph.vertex.resolver.create.on.msgs",
                    true,
                    "Option of whether to create vertexes that were not existent "
                            + "before but received messages");

    /**
     * Observer class to watch over job status - optional
     */
    ClassConfOption<GiraphJobObserver> JOB_OBSERVER_CLASS =
            ClassConfOption.create(
                    "giraph.jobObserverClass",
                    DefaultJobObserver.class,
                    GiraphJobObserver.class,
                    "Observer class to watch over job status - optional");

    /**
     * Observer class to watch over job status - optional
     */
    ClassConfOption<GiraphJobRetryChecker> JOB_RETRY_CHECKER_CLASS =
            ClassConfOption.create(
                    "giraph.jobRetryCheckerClass",
                    DefaultGiraphJobRetryChecker.class,
                    GiraphJobRetryChecker.class,
                    "Class which decides whether a failed job should be retried - " + "optional");

    /**
     * Maximum allowed time for job to run after getting all resources before it will be killed, in
     * milliseconds (-1 if it has no limit)
     */
    LongConfOption MAX_ALLOWED_JOB_TIME_MS =
            new LongConfOption(
                    "giraph.maxAllowedJobTimeMilliseconds",
                    -1,
                    "Maximum allowed time for job to run after getting all resources "
                            + "before it will be killed, in milliseconds "
                            + "(-1 if it has no limit)");

    /**
     * EdgeOutputFormat sub-directory
     */
    StrConfOption EDGE_OUTPUT_FORMAT_SUBDIR =
            new StrConfOption("giraph.edge.output.subdir", "", "EdgeOutputFormat sub-directory");

    /**
     * GiraphTextOuputFormat Separator
     */
    StrConfOption GIRAPH_TEXT_OUTPUT_FORMAT_SEPARATOR =
            new StrConfOption(
                    "giraph.textoutputformat.separator", "\t", "GiraphTextOuputFormat Separator");
    /**
     * Reverse values in the output
     */
    BooleanConfOption GIRAPH_TEXT_OUTPUT_FORMAT_REVERSE =
            new BooleanConfOption(
                    "giraph.textoutputformat.reverse", false, "Reverse values in the output");

    /**
     * If you use this option, instead of having saving vertices in the end of application,
     * saveVertex will be called right after each vertex.compute() is called. NOTE: This feature
     * doesn't work well with checkpointing - if you restart from a checkpoint you won't have any
     * output from previous supersteps.
     */
    BooleanConfOption DO_OUTPUT_DURING_COMPUTATION =
            new BooleanConfOption(
                    "giraph.doOutputDuringComputation",
                    false,
                    "If you use this option, instead of having saving vertices in the "
                            + "end of application, saveVertex will be called right after each "
                            + "vertex.compute() is called."
                            + "NOTE: This feature doesn't work well with checkpointing - if you "
                            + "restart from a checkpoint you won't have any ouptut from previous "
                            + "supresteps.");
    /**
     * Vertex output format thread-safe - if your VertexOutputFormat allows several vertexWriters to
     * be created and written to in parallel, you should set this to true.
     */
    BooleanConfOption VERTEX_OUTPUT_FORMAT_THREAD_SAFE =
            new BooleanConfOption(
                    "giraph.vertexOutputFormatThreadSafe",
                    false,
                    "Vertex output format thread-safe - if your VertexOutputFormat "
                            + "allows several vertexWriters to be created and written to in "
                            + "parallel, you should set this to true.");
    /**
     * Number of threads for writing output in the end of the application
     */
    IntConfOption NUM_OUTPUT_THREADS =
            new IntConfOption(
                    "giraph.numOutputThreads",
                    1,
                    "Number of threads for writing output in the end of the application");

    /**
     * conf key for comma-separated list of jars to export to YARN workers
     */
    StrConfOption GIRAPH_YARN_LIBJARS =
            new StrConfOption(
                    "giraph.yarn.libjars",
                    "",
                    "conf key for comma-separated list of jars to export to YARN workers");
    /**
     * Name of the XML file that will export our Configuration to YARN workers
     */
    String GIRAPH_YARN_CONF_FILE = "giraph-conf.xml";
    /**
     * Giraph default heap size for all tasks when running on YARN profile
     */
    int GIRAPH_YARN_TASK_HEAP_MB_DEFAULT = 1024;
    /**
     * Name of Giraph property for user-configurable heap memory per worker
     */
    IntConfOption GIRAPH_YARN_TASK_HEAP_MB =
            new IntConfOption(
                    "giraph.yarn.task.heap.mb",
                    GIRAPH_YARN_TASK_HEAP_MB_DEFAULT,
                    "Name of Giraph property for user-configurable heap memory per worker");
    /**
     * Default priority level in YARN for our task containers
     */
    int GIRAPH_YARN_PRIORITY = 10;
    /**
     * Is this a pure YARN job (i.e. no MapReduce layer managing Giraph tasks)
     */
    BooleanConfOption IS_PURE_YARN_JOB =
            new BooleanConfOption(
                    "giraph.pure.yarn.job",
                    false,
                    "Is this a pure YARN job (i.e. no MapReduce layer managing Giraph " + "tasks)");

    /**
     * Minimum number of simultaneous workers before this job can run (int)
     */
    String MIN_WORKERS = "giraph.minWorkers";

    /**
     * Separate the workers and the master tasks.  This is required to support dynamic recovery.
     * (boolean)
     */
    BooleanConfOption SPLIT_MASTER_WORKER =
            new BooleanConfOption(
                    "giraph.SplitMasterWorker",
                    true,
                    "Separate the workers and the master tasks.  This is required to "
                            + "support dynamic recovery. (boolean)");

    /**
     * Indicates whether this job is run in an internal unit test
     */
    BooleanConfOption LOCAL_TEST_MODE =
            new BooleanConfOption(
                    "giraph.localTestMode",
                    false,
                    "Indicates whether this job is run in an internal unit test");

    /**
     * Override the Hadoop log level and set the desired log level.
     */
    StrConfOption LOG_LEVEL =
            new StrConfOption(
                    "giraph.logLevel",
                    "info",
                    "Override the Hadoop log level and set the desired log level.");

    /**
     * Use thread level debugging?
     */
    BooleanConfOption LOG_THREAD_LAYOUT =
            new BooleanConfOption("giraph.logThreadLayout", false, "Use thread level debugging?");

    /**
     * Configuration key to enable jmap printing
     */
    BooleanConfOption JMAP_ENABLE =
            new BooleanConfOption(
                    "giraph.jmap.histo.enable", false, "Configuration key to enable jmap printing");

    /**
     * Configuration key for msec to sleep between calls
     */
    IntConfOption JMAP_SLEEP_MILLIS =
            new IntConfOption(
                    "giraph.jmap.histo.msec",
                    SECONDS.toMillis(30),
                    "Configuration key for msec to sleep between calls");

    /**
     * Configuration key for how many lines to print
     */
    IntConfOption JMAP_PRINT_LINES =
            new IntConfOption(
                    "giraph.jmap.histo.print_lines",
                    30,
                    "Configuration key for how many lines to print");

    /**
     * Configuration key for printing live objects only This option will trigger Full GC for every
     * jmap dump and so can significantly hinder performance.
     */
    BooleanConfOption JMAP_LIVE_ONLY =
            new BooleanConfOption(
                    "giraph.jmap.histo.live", false, "Only print live objects in jmap?");

    /**
     * Option used by ReactiveJMapHistoDumper to check for an imminent OOM in worker or master
     * process
     */
    IntConfOption MIN_FREE_MBS_ON_HEAP =
            new IntConfOption(
                    "giraph.heap.minFreeMb",
                    128,
                    "Option used by "
                            + "worker and master observers to check for imminent OOM exception");
    /**
     * Option can be used to enable reactively dumping jmap histo when OOM is imminent
     */
    BooleanConfOption REACTIVE_JMAP_ENABLE =
            new BooleanConfOption(
                    "giraph.heap.enableReactiveJmapDumping",
                    false,
                    "Option to enable dumping jmap histogram reactively based on "
                            + "free memory on heap");

    /**
     * Minimum percent of the maximum number of workers that have responded in order to continue
     * progressing. (float)
     */
    FloatConfOption MIN_PERCENT_RESPONDED =
            new FloatConfOption(
                    "giraph.minPercentResponded",
                    100.0f,
                    "Minimum percent of the maximum number of workers that have "
                            + "responded in order to continue progressing. (float)");

    /**
     * Enable the Metrics system
     */
    BooleanConfOption METRICS_ENABLE =
            new BooleanConfOption("giraph.metrics.enable", false, "Enable the Metrics system");

    /**
     * Directory in HDFS to write master metrics to, instead of stderr
     */
    StrConfOption METRICS_DIRECTORY =
            new StrConfOption(
                    "giraph.metrics.directory",
                    "",
                    "Directory in HDFS to write master metrics to, instead of stderr");

    /**
     * ZooKeeper comma-separated list (if not set, will start up ZooKeeper locally). Consider that
     * after locally-starting zookeeper, this parameter will updated the configuration with the
     * corrent configuration value.
     */
    StrConfOption ZOOKEEPER_LIST =
            new StrConfOption(
                    "giraph.zkList",
                    "",
                    "ZooKeeper comma-separated list (if not set, will start up "
                            + "ZooKeeper locally). Consider that after locally-starting "
                            + "zookeeper, this parameter will updated the configuration with "
                            + "the corrent configuration value.");

    /**
     * Zookeeper List will always hold a value during the computation while this option provides
     * information regarding whether the zookeeper was internally started or externally provided.
     */
    BooleanConfOption ZOOKEEPER_IS_EXTERNAL =
            new BooleanConfOption(
                    "giraph.zkIsExternal",
                    true,
                    "Zookeeper List will always hold a value during "
                            + "the computation while this option provides "
                            + "information regarding whether the zookeeper was "
                            + "internally started or externally provided.");

    /**
     * ZooKeeper session millisecond timeout
     */
    IntConfOption ZOOKEEPER_SESSION_TIMEOUT =
            new IntConfOption(
                    "giraph.zkSessionMsecTimeout",
                    MINUTES.toMillis(1),
                    "ZooKeeper session millisecond timeout");

    /**
     * Polling interval to check for the ZooKeeper server data
     */
    IntConfOption ZOOKEEPER_SERVERLIST_POLL_MSECS =
            new IntConfOption(
                    "giraph.zkServerlistPollMsecs",
                    SECONDS.toMillis(3),
                    "Polling interval to check for the ZooKeeper server data");

    /**
     * ZooKeeper port to use
     */
    IntConfOption ZOOKEEPER_SERVER_PORT =
            new IntConfOption("giraph.zkServerPort", 22181, "ZooKeeper port to use");

    /**
     * Local ZooKeeper directory to use
     */
    String ZOOKEEPER_DIR = "giraph.zkDir";

    /**
     * Max attempts for handling ZooKeeper connection loss
     */
    IntConfOption ZOOKEEPER_OPS_MAX_ATTEMPTS =
            new IntConfOption(
                    "giraph.zkOpsMaxAttempts",
                    3,
                    "Max attempts for handling ZooKeeper connection loss");

    /**
     * Msecs to wait before retrying a failed ZooKeeper op due to connection loss.
     */
    IntConfOption ZOOKEEPER_OPS_RETRY_WAIT_MSECS =
            new IntConfOption(
                    "giraph.zkOpsRetryWaitMsecs",
                    SECONDS.toMillis(5),
                    "Msecs to wait before retrying a failed ZooKeeper op due to "
                            + "connection loss.");

    /**
     * Netty server threads
     */
    IntConfOption NETTY_SERVER_THREADS =
            new IntConfOption("giraph.nettyServerThreads", 16, "Netty server threads");

    /**
     * Max resolve address attempts
     */
    IntConfOption MAX_RESOLVE_ADDRESS_ATTEMPTS =
            new IntConfOption(
                    "giraph.maxResolveAddressAttempts", 5, "Max resolve address attempts");

    /**
     * Millseconds to wait for an event before continuing
     */
    IntConfOption EVENT_WAIT_MSECS =
            new IntConfOption(
                    "giraph.eventWaitMsecs",
                    SECONDS.toMillis(30),
                    "Millseconds to wait for an event before continuing");

    /**
     * Maximum milliseconds to wait before giving up trying to get the minimum number of workers
     * before a superstep (int).
     */
    IntConfOption MAX_MASTER_SUPERSTEP_WAIT_MSECS =
            new IntConfOption(
                    "giraph.maxMasterSuperstepWaitMsecs",
                    MINUTES.toMillis(10),
                    "Maximum milliseconds to wait before giving up trying to get the "
                            + "minimum number of workers before a superstep (int).");

    /**
     * Maximum milliseconds to wait before giving up waiting for the workers to write the counters
     * to the Zookeeper after a superstep
     */
    IntConfOption MAX_COUNTER_WAIT_MSECS =
            new IntConfOption(
                    "giraph.maxCounterWaitMsecs",
                    MINUTES.toMillis(2),
                    "Maximum milliseconds to wait before giving up waiting for"
                            + "the workers to write their counters to the "
                            + "zookeeper after a superstep");

    /**
     * Milliseconds for a request to complete (or else resend)
     */
    IntConfOption MAX_REQUEST_MILLISECONDS =
            new IntConfOption(
                    "giraph.maxRequestMilliseconds",
                    MINUTES.toMillis(10),
                    "Milliseconds for a request to complete (or else resend)");

    /**
     * Whether to resend request which timed out or fail the job if timeout happens
     */
    BooleanConfOption RESEND_TIMED_OUT_REQUESTS =
            new BooleanConfOption(
                    "giraph.resendTimedOutRequests",
                    true,
                    "Whether to resend request which timed out or fail the job if "
                            + "timeout happens");

    /**
     * Netty max connection failures
     */
    IntConfOption NETTY_MAX_CONNECTION_FAILURES =
            new IntConfOption(
                    "giraph.nettyMaxConnectionFailures", 1000, "Netty max connection failures");

    /**
     * How long to wait before trying to reconnect failed connections
     */
    IntConfOption WAIT_TIME_BETWEEN_CONNECTION_RETRIES_MS =
            new IntConfOption("giraph.waitTimeBetweenConnectionRetriesMs", 500, "");
    /**
     * Fail first IPC port binding attempt, simulate binding failure on real grid testing
     */
    BooleanConfOption FAIL_FIRST_IPC_PORT_BIND_ATTEMPT =
            new BooleanConfOption(
                    "giraph.failFirstIpcPortBindAttempt",
                    false,
                    "Fail first IPC port binding attempt, simulate binding failure "
                            + "on real grid testing");

    /**
     * Maximum size of messages (in bytes) per peer before flush
     */
    IntConfOption MAX_MSG_REQUEST_SIZE =
            new IntConfOption(
                    "giraph.msgRequestSize",
                    512 * ONE_KB,
                    "Maximum size of messages (in bytes) per peer before flush");

    /**
     * How much bigger than the average per partition size to make initial per partition buffers. If
     * this value is A, message request size is M, and a worker has P partitions, than its initial
     * partition buffer size will be (M / P) * (1 + A).
     */
    FloatConfOption ADDITIONAL_MSG_REQUEST_SIZE =
            new FloatConfOption(
                    "giraph.additionalMsgRequestSize",
                    0.2f,
                    "How much bigger than the average per partition size to make "
                            + "initial per partition buffers. If this value is A, message "
                            + "request size is M, and a worker has P partitions, than its "
                            + "initial partition buffer size will be (M / P) * (1 + A).");

    /**
     * Maximum size of vertices (in bytes) per peer before flush
     */
    IntConfOption MAX_VERTEX_REQUEST_SIZE =
            new IntConfOption(
                    "giraph.vertexRequestSize",
                    512 * ONE_KB,
                    "Maximum size of vertices (in bytes) per peer before flush");

    /**
     * Additional size (expressed as a ratio) of each per-partition buffer on top of the average
     * size for vertices.
     */
    FloatConfOption ADDITIONAL_VERTEX_REQUEST_SIZE =
            new FloatConfOption(
                    "giraph.additionalVertexRequestSize",
                    0.2f,
                    "Additional size (expressed as a ratio) of each per-partition "
                            + "buffer on top of the average size.");

    /**
     * Maximum size of edges (in bytes) per peer before flush
     */
    IntConfOption MAX_EDGE_REQUEST_SIZE =
            new IntConfOption(
                    "giraph.edgeRequestSize",
                    512 * ONE_KB,
                    "Maximum size of edges (in bytes) per peer before flush");

    /**
     * Additional size (expressed as a ratio) of each per-partition buffer on top of the average
     * size for edges.
     */
    FloatConfOption ADDITIONAL_EDGE_REQUEST_SIZE =
            new FloatConfOption(
                    "giraph.additionalEdgeRequestSize",
                    0.2f,
                    "Additional size (expressed as a ratio) of each per-partition "
                            + "buffer on top of the average size.");

    /**
     * Maximum number of mutations per partition before flush
     */
    IntConfOption MAX_MUTATIONS_PER_REQUEST =
            new IntConfOption(
                    "giraph.maxMutationsPerRequest",
                    100,
                    "Maximum number of mutations per partition before flush");

    /**
     * Use message size encoding (typically better for complex objects, not meant for primitive
     * wrapped messages)
     */
    BooleanConfOption USE_MESSAGE_SIZE_ENCODING =
            new BooleanConfOption(
                    "giraph.useMessageSizeEncoding",
                    false,
                    "Use message size encoding (typically better for complex objects, "
                            + "not meant for primitive wrapped messages)");

    /**
     * Number of flush threads per peer
     */
    String MSG_NUM_FLUSH_THREADS = "giraph.msgNumFlushThreads";

    /**
     * Number of threads for vertex computation
     */
    IntConfOption NUM_COMPUTE_THREADS =
            new IntConfOption(
                    "giraph.numComputeThreads", 1, "Number of threads for vertex computation");

    /**
     * Number of threads for input split loading
     */
    IntConfOption NUM_INPUT_THREADS =
            new IntConfOption(
                    "giraph.numInputThreads", 1, "Number of threads for input split loading");

    /**
     * Minimum stragglers of the superstep before printing them out
     */
    IntConfOption PARTITION_LONG_TAIL_MIN_PRINT =
            new IntConfOption(
                    "giraph.partitionLongTailMinPrint",
                    1,
                    "Minimum stragglers of the superstep before printing them out");

    /**
     * Use superstep counters? (boolean)
     */
    BooleanConfOption USE_SUPERSTEP_COUNTERS =
            new BooleanConfOption(
                    "giraph.useSuperstepCounters", true, "Use superstep counters? (boolean)");

    /**
     * Input split sample percent - Used only for sampling and testing, rather than an actual job.
     * The idea is that to test, you might only want a fraction of the actual input splits from your
     * VertexInputFormat to load (values should be [0, 100]).
     */
    FloatConfOption INPUT_SPLIT_SAMPLE_PERCENT =
            new FloatConfOption(
                    "giraph.inputSplitSamplePercent",
                    100f,
                    "Input split sample percent - Used only for sampling and testing, "
                            + "rather than an actual job.  The idea is that to test, you might "
                            + "only want a fraction of the actual input splits from your "
                            + "VertexInputFormat to load (values should be [0, 100]).");

    /**
     * To limit outlier vertex input splits from producing too many vertices or to help with
     * testing, the number of vertices loaded from an input split can be limited.  By default,
     * everything is loaded.
     */
    LongConfOption INPUT_SPLIT_MAX_VERTICES =
            new LongConfOption(
                    "giraph.InputSplitMaxVertices",
                    -1,
                    "To limit outlier vertex input splits from producing too many "
                            + "vertices or to help with testing, the number of vertices "
                            + "loaded from an input split can be limited. By default, "
                            + "everything is loaded.");

    /**
     * To limit outlier vertex input splits from producing too many vertices or to help with
     * testing, the number of edges loaded from an input split can be limited.  By default,
     * everything is loaded.
     */
    LongConfOption INPUT_SPLIT_MAX_EDGES =
            new LongConfOption(
                    "giraph.InputSplitMaxEdges",
                    -1,
                    "To limit outlier vertex input splits from producing too many "
                            + "vertices or to help with testing, the number of edges loaded "
                            + "from an input split can be limited. By default, everything is "
                            + "loaded.");

    /**
     * To minimize network usage when reading input splits, each worker can prioritize splits that
     * reside on its host. This, however, comes at the cost of increased load on ZooKeeper. Hence,
     * users with a lot of splits and input threads (or with configurations that can't exploit
     * locality) may want to disable it.
     */
    BooleanConfOption USE_INPUT_SPLIT_LOCALITY =
            new BooleanConfOption(
                    "giraph.useInputSplitLocality",
                    true,
                    "To minimize network usage when reading input splits, each worker "
                            + "can prioritize splits that reside on its host. "
                            + "This, however, comes at the cost of increased load on ZooKeeper. "
                            + "Hence, users with a lot of splits and input threads (or with "
                            + "configurations that can't exploit locality) may want to disable "
                            + "it.");

    /**
     * Multiplier for the current workers squared
     */
    FloatConfOption PARTITION_COUNT_MULTIPLIER =
            new FloatConfOption(
                    "giraph.masterPartitionCountMultiplier",
                    1.0f,
                    "Multiplier for the current workers squared");

    /**
     * Minimum number of partitions to have per compute thread
     */
    IntConfOption MIN_PARTITIONS_PER_COMPUTE_THREAD =
            new IntConfOption(
                    "giraph.minPartitionsPerComputeThread",
                    1,
                    "Minimum number of partitions to have per compute thread");

    /**
     * Overrides default partition count calculation if not -1
     */
    IntConfOption USER_PARTITION_COUNT =
            new IntConfOption(
                    "giraph.userPartitionCount",
                    -1,
                    "Overrides default partition count calculation if not -1");

    /**
     * Vertex key space size for {@link org.apache.giraph.partition.WorkerGraphPartitionerImpl}
     */
    String PARTITION_VERTEX_KEY_SPACE_SIZE = "giraph.vertexKeySpaceSize";

    /**
     * How often to checkpoint (i.e. 0, means no checkpoint, 1 means every superstep, 2 is every two
     * supersteps, etc.).
     */
    IntConfOption CHECKPOINT_FREQUENCY =
            new IntConfOption(
                    "giraph.checkpointFrequency",
                    0,
                    "How often to checkpoint (i.e. 0, means no checkpoint, 1 means "
                            + "every superstep, 2 is every two supersteps, etc.).");

    /**
     * Delete checkpoints after a successful job run?
     */
    BooleanConfOption CLEANUP_CHECKPOINTS_AFTER_SUCCESS =
            new BooleanConfOption(
                    "giraph.cleanupCheckpointsAfterSuccess",
                    true,
                    "Delete checkpoints after a successful job run?");

    /**
     * An application can be restarted manually by selecting a superstep.  The corresponding
     * checkpoint must exist for this to work.  The user should set a long value.  Default is start
     * from scratch.
     */
    String RESTART_SUPERSTEP = "giraph.restartSuperstep";

    /**
     * If application is restarted manually we need to specify job ID to restart from.
     */
    StrConfOption RESTART_JOB_ID =
            new StrConfOption(
                    "giraph.restart.jobId", null, "Which job ID should I try to restart?");

    /**
     * Base ZNode for Giraph's state in the ZooKeeper cluster.  Must be a root znode on the cluster
     * beginning with "/"
     */
    String BASE_ZNODE_KEY = "giraph.zkBaseZNode";

    /**
     * If ZOOKEEPER_LIST is not set, then use this directory to manage ZooKeeper
     */
    StrConfOption ZOOKEEPER_MANAGER_DIRECTORY =
            new StrConfOption(
                    "giraph.zkManagerDirectory",
                    "_bsp/_defaultZkManagerDir",
                    "If ZOOKEEPER_LIST is not set, then use this directory to manage "
                            + "ZooKeeper");

    /**
     * Number of ZooKeeper client connection attempts before giving up.
     */
    IntConfOption ZOOKEEPER_CONNECTION_ATTEMPTS =
            new IntConfOption(
                    "giraph.zkConnectionAttempts",
                    10,
                    "Number of ZooKeeper client connection attempts before giving up.");

    /**
     * This directory has/stores the available checkpoint files in HDFS.
     */
    StrConfOption CHECKPOINT_DIRECTORY =
            new StrConfOption(
                    "giraph.checkpointDirectory",
                    "_bsp/_checkpoints/",
                    "This directory has/stores the available checkpoint files in HDFS.");

    /**
     * Comma-separated list of directories in the local filesystem for out-of-core partitions.
     */
    StrConfOption PARTITIONS_DIRECTORY =
            new StrConfOption(
                    "giraph.partitionsDirectory",
                    "_bsp/_partitions",
                    "Comma-separated list of directories in the local filesystem for "
                            + "out-of-core partitions.");

    /**
     * Number of IO threads used in out-of-core mechanism. If local disk is used for spilling data
     * to and reading data from, this number should be equal to the number of available disks on
     * each machine. In such case, one should use giraph.partitionsDirectory to specify directories
     * mounted on different disks.
     */
    IntConfOption NUM_OUT_OF_CORE_THREADS =
            new IntConfOption(
                    "giraph.numOutOfCoreThreads",
                    1,
                    "Number of IO "
                            + "threads used in out-of-core mechanism. If using local disk to "
                            + "spill data, this should be equal to the number of available "
                            + "disks. In such case, use giraph.partitionsDirectory to specify "
                            + "mount points on different disks.");

    /**
     * Enable out-of-core graph.
     */
    BooleanConfOption USE_OUT_OF_CORE_GRAPH =
            new BooleanConfOption("giraph.useOutOfCoreGraph", false, "Enable out-of-core graph.");

    /**
     * Data accessor resource/object
     */
    ClassConfOption<OutOfCoreDataAccessor> OUT_OF_CORE_DATA_ACCESSOR =
            ClassConfOption.create(
                    "giraph.outOfCoreDataAccessor",
                    LocalDiskDataAccessor.class,
                    OutOfCoreDataAccessor.class,
                    "Data accessor used in out-of-core computation (local-disk, "
                            + "in-memory, HDFS, etc.)");

    /**
     * Out-of-core oracle that is to be used for adaptive out-of-core engine. If the
     * `MAX_PARTITIONS_IN_MEMORY` is already set, this will be over-written to be
     * `FixedPartitionsOracle`.
     */
    ClassConfOption<OutOfCoreOracle> OUT_OF_CORE_ORACLE =
            ClassConfOption.create(
                    "giraph.outOfCoreOracle",
                    MemoryEstimatorOracle.class,
                    OutOfCoreOracle.class,
                    "Out-of-core oracle that is to be used for adaptive out-of-core " + "engine");

    /**
     * Maximum number of partitions to hold in memory for each worker.
     */
    IntConfOption MAX_PARTITIONS_IN_MEMORY =
            new IntConfOption(
                    "giraph.maxPartitionsInMemory",
                    0,
                    "Maximum number of partitions to hold in memory for each worker. By"
                            + " default it is set to 0 (for adaptive out-of-core mechanism");

    /**
     * Directory to write YourKit snapshots to
     */
    String YOURKIT_OUTPUT_DIR = "giraph.yourkit.outputDir";
    /**
     * Default directory to write YourKit snapshots to
     */
    String YOURKIT_OUTPUT_DIR_DEFAULT = "/tmp/giraph/%JOB_ID%/%TASK_ID%";

    /**
     * Keep the zookeeper output for debugging? Default is to remove it.
     */
    BooleanConfOption KEEP_ZOOKEEPER_DATA =
            new BooleanConfOption(
                    "giraph.keepZooKeeperData",
                    false,
                    "Keep the zookeeper output for debugging? Default is to remove it.");
    /**
     * Default ZooKeeper snap count.
     */
    int DEFAULT_ZOOKEEPER_SNAP_COUNT = 50000;
    /**
     * Default ZooKeeper tick time.
     */
    int DEFAULT_ZOOKEEPER_TICK_TIME = 6000;
    /**
     * Default ZooKeeper maximum client connections.
     */
    int DEFAULT_ZOOKEEPER_MAX_CLIENT_CNXNS = 10000;
    /**
     * Number of snapshots to be retained after purge
     */
    int ZOOKEEPER_SNAP_RETAIN_COUNT = 3;
    /**
     * Zookeeper purge interval in hours
     */
    int ZOOKEEPER_PURGE_INTERVAL = 1;
    /**
     * ZooKeeper minimum session timeout
     */
    IntConfOption ZOOKEEPER_MIN_SESSION_TIMEOUT =
            new IntConfOption(
                    "giraph.zKMinSessionTimeout",
                    MINUTES.toMillis(10),
                    "ZooKeeper minimum session timeout");
    /**
     * ZooKeeper maximum session timeout
     */
    IntConfOption ZOOKEEPER_MAX_SESSION_TIMEOUT =
            new IntConfOption(
                    "giraph.zkMaxSessionTimeout",
                    MINUTES.toMillis(15),
                    "ZooKeeper maximum session timeout");

    /**
     * ZooKeeper force sync
     */
    BooleanConfOption ZOOKEEPER_FORCE_SYNC =
            new BooleanConfOption("giraph.zKForceSync", false, "ZooKeeper force sync");

    /**
     * ZooKeeper skip ACLs
     */
    BooleanConfOption ZOOKEEPER_SKIP_ACL =
            new BooleanConfOption("giraph.ZkSkipAcl", true, "ZooKeeper skip ACLs");

    /**
     * Whether to use SASL with DIGEST and Hadoop Job Tokens to authenticate and authorize Netty BSP
     * Clients to Servers.
     */
    BooleanConfOption AUTHENTICATE =
            new BooleanConfOption(
                    "giraph.authenticate",
                    false,
                    "Whether to use SASL with DIGEST and Hadoop Job Tokens to "
                            + "authenticate and authorize Netty BSP Clients to Servers.");

    /**
     * Whether to use SSL to authenticate and authorize " Netty BSP Clients to Servers.
     */
    BooleanConfOption SSL_ENCRYPT =
            new BooleanConfOption(
                    "giraph.sslEncrypt",
                    false,
                    "Whether to use SSL to authenticate and authorize "
                            + "Netty BSP Clients to Servers.");

    /**
     * Use BigDataIO for messages? If there are super-vertices in the graph which receive a lot of
     * messages (total serialized size of messages goes beyond the maximum size of a byte array),
     * setting this option to true will remove that limit. The maximum memory available for a single
     * vertex will be limited to the maximum heap size available.
     */
    BooleanConfOption USE_BIG_DATA_IO_FOR_MESSAGES =
            new BooleanConfOption(
                    "giraph.useBigDataIOForMessages", false, "Use BigDataIO for messages?");

    /**
     * Maximum number of attempts a master/worker will retry before killing the job.  This directly
     * maps to the number of map task attempts in Hadoop.
     */
    IntConfOption MAX_TASK_ATTEMPTS =
            new IntConfOption(
                    "mapred.map.max.attempts",
                    -1,
                    "Maximum number of attempts a master/worker will retry before "
                            + "killing the job.  This directly maps to the number of map task "
                            + "attempts in Hadoop.");

    /**
     * Interface to use for hostname resolution
     */
    StrConfOption DNS_INTERFACE =
            new StrConfOption(
                    "giraph.dns.interface", "default", "Interface to use for hostname resolution");
    /**
     * Server for hostname resolution
     */
    StrConfOption DNS_NAMESERVER =
            new StrConfOption("giraph.dns.nameserver", "default", "Server for hostname resolution");

    /**
     * The application will halt after this many supersteps is completed.  For instance, if it is
     * set to 3, the application will run at most 0, 1, and 2 supersteps and then go into the
     * shutdown superstep.
     */
    IntConfOption MAX_NUMBER_OF_SUPERSTEPS =
            new IntConfOption(
                    "giraph.maxNumberOfSupersteps",
                    1,
                    "The application will halt after this many supersteps is "
                            + "completed. For instance, if it is set to 3, the application will "
                            + "run at most 0, 1, and 2 supersteps and then go into the shutdown "
                            + "superstep.");

    /**
     * The application will not mutate the graph topology (the edges). It is used to optimise
     * out-of-core graph, by not writing back edges every time.
     */
    BooleanConfOption STATIC_GRAPH =
            new BooleanConfOption(
                    "giraph.isStaticGraph",
                    false,
                    "The application will not mutate the graph topology (the edges). "
                            + "It is used to optimise out-of-core graph, by not writing back "
                            + "edges every time.");

    /**
     * This option can be used to specify if a source vertex present in edge input but not in vertex
     * input can be created
     */
    BooleanConfOption CREATE_EDGE_SOURCE_VERTICES =
            new BooleanConfOption(
                    "giraph.createEdgeSourceVertices",
                    true,
                    "Create a source vertex if present in edge input but not "
                            + "necessarily in vertex input");

    /**
     * Defines a call back that can be used to make decisions on whether the vertex should be
     * created or not in the runtime.
     */
    ClassConfOption<CreateSourceVertexCallback> CREATE_EDGE_SOURCE_VERTICES_CALLBACK =
            ClassConfOption.create(
                    "giraph.createEdgeSourceVerticesCallback",
                    DefaultCreateSourceVertexCallback.class,
                    CreateSourceVertexCallback.class,
                    "Decide whether we should create a source vertex when id is "
                            + "present in the edge input but not in vertex input");

    /**
     * This counter group will contain one counter whose name is the ZooKeeper server:port which
     * this job is using
     */
    String ZOOKEEPER_SERVER_PORT_COUNTER_GROUP = "Zookeeper server:port";

    /**
     * This counter group will contain one counter whose name is the ZooKeeper node path which
     * should be created to trigger computation halt
     */
    String ZOOKEEPER_HALT_NODE_COUNTER_GROUP = "Zookeeper halt node";

    /**
     * This counter group will contain one counter whose name is the ZooKeeper node path which
     * contains all data about this job
     */
    String ZOOKEEPER_BASE_PATH_COUNTER_GROUP = "Zookeeper base path";

    /**
     * Which class to use to write instructions on how to halt the application
     */
    ClassConfOption<HaltApplicationUtils.HaltInstructionsWriter> HALT_INSTRUCTIONS_WRITER_CLASS =
            ClassConfOption.create(
                    "giraph.haltInstructionsWriter",
                    HaltApplicationUtils.DefaultHaltInstructionsWriter.class,
                    HaltApplicationUtils.HaltInstructionsWriter.class,
                    "Class used to write instructions on how to halt the application");

    /**
     * Maximum timeout (in milliseconds) for waiting for all tasks to complete after the job is
     * done.  Defaults to 15 minutes.
     */
    IntConfOption WAIT_TASK_DONE_TIMEOUT_MS =
            new IntConfOption(
                    "giraph.waitTaskDoneTimeoutMs",
                    MINUTES.toMillis(15),
                    "Maximum timeout (in ms) for waiting for all all tasks to " + "complete");

    /**
     * Whether to track job progress on client or not
     */
    BooleanConfOption TRACK_JOB_PROGRESS_ON_CLIENT =
            new BooleanConfOption(
                    "giraph.trackJobProgressOnClient",
                    false,
                    "Whether to track job progress on client or not");

    /**
     * Class to use as the job progress client
     */
    ClassConfOption<JobProgressTrackerClient> JOB_PROGRESS_TRACKER_CLIENT_CLASS =
            ClassConfOption.create(
                    "giraph.jobProgressTrackerClientClass",
                    RetryableJobProgressTrackerClient.class,
                    JobProgressTrackerClient.class,
                    "Class to use to make calls to the job progress tracker service");

    /**
     * Class to use to track job progress on client
     */
    ClassConfOption<JobProgressTrackerService> JOB_PROGRESS_TRACKER_SERVICE_CLASS =
            ClassConfOption.create(
                    "giraph.jobProgressTrackerServiceClass",
                    DefaultJobProgressTrackerService.class,
                    JobProgressTrackerService.class,
                    "Class to use to track job progress on client");

    /**
     * Minimum number of vertices to compute before adding to worker progress.
     */
    LongConfOption VERTICES_TO_UPDATE_PROGRESS =
            new LongConfOption(
                    "giraph.VerticesToUpdateProgress",
                    100000,
                    "Minimum number of vertices to compute before " + "updating worker progress");

    /**
     * Number of retries for creating the HDFS files
     */
    IntConfOption HDFS_FILE_CREATION_RETRIES =
            new IntConfOption(
                    "giraph.hdfs.file.creation.retries",
                    10,
                    "Retries to create an HDFS file before failing");

    /**
     * Number of milliseconds to wait before retrying HDFS file creation
     */
    IntConfOption HDFS_FILE_CREATION_RETRY_WAIT_MS =
            new IntConfOption(
                    "giraph.hdfs.file.creation.retry.wait.ms",
                    30_000,
                    "Milliseconds to wait prior to retrying creation of an HDFS file");

    /**
     * Number of threads for writing and reading checkpoints
     */
    IntConfOption NUM_CHECKPOINT_IO_THREADS =
            new IntConfOption(
                    "giraph.checkpoint.io.threads",
                    8,
                    "Number of threads for writing and reading checkpoints");

    /**
     * Compression algorithm to be used for checkpointing. Defined by extension for hadoop
     * compatibility reasons.
     */
    StrConfOption CHECKPOINT_COMPRESSION_CODEC =
            new StrConfOption(
                    "giraph.checkpoint.compression.codec",
                    ".deflate",
                    "Defines compression algorithm we will be using for "
                            + "storing checkpoint. Available options include but "
                            + "not restricted to: .deflate, .gz, .bz2, .lzo");

    /**
     * Defines if and when checkpointing is supported by this job. By default checkpointing is
     * always supported unless output during the computation is enabled.
     */
    ClassConfOption<CheckpointSupportedChecker> CHECKPOINT_SUPPORTED_CHECKER =
            ClassConfOption.create(
                    "giraph.checkpoint.supported.checker",
                    DefaultCheckpointSupportedChecker.class,
                    CheckpointSupportedChecker.class,
                    "This is the way to specify if checkpointing is " + "supported by the job");

    /**
     * Number of threads to use in async message store, 0 means we should not use async message
     * processing
     */
    IntConfOption ASYNC_MESSAGE_STORE_THREADS_COUNT =
            new IntConfOption(
                    "giraph.async.message.store.threads",
                    0,
                    "Number of threads to be used in async message store.");

    /**
     * Output format class for hadoop to use (for committing)
     */
    ClassConfOption<OutputFormat> HADOOP_OUTPUT_FORMAT_CLASS =
            ClassConfOption.create(
                    "giraph.hadoopOutputFormatClass",
                    BspOutputFormat.class,
                    OutputFormat.class,
                    "Output format class for hadoop to use (for committing)");

    /**
     * For worker to worker communication we can use IPs or host names, by default prefer IPs.
     */
    BooleanConfOption PREFER_IP_ADDRESSES =
            new BooleanConfOption(
                    "giraph.preferIP", false, "Prefer IP addresses instead of host names");

    /**
     * Timeout for "waitForever", when we need to wait for zookeeper. Since we should never really
     * have to wait forever. We should only wait some reasonable but large amount of time.
     */
    LongConfOption WAIT_ZOOKEEPER_TIMEOUT_MSEC =
            new LongConfOption(
                    "giraph.waitZookeeperTimeoutMsec",
                    MINUTES.toMillis(15),
                    "How long should we stay in waitForever loops in various "
                            + "places that require network connection");

    /**
     * Timeout for "waitForever", when we need to wait for other workers to complete their job.
     * Since we should never really have to wait forever. We should only wait some reasonable but
     * large amount of time.
     */
    LongConfOption WAIT_FOR_OTHER_WORKERS_TIMEOUT_MSEC =
            new LongConfOption(
                    "giraph.waitForOtherWorkersMsec",
                    HOURS.toMillis(48),
                    "How long should workers wait to finish superstep");

    /**
     * Number of supersteps job will run for
     */
    IntConfOption SUPERSTEP_COUNT =
            new IntConfOption("giraph.numSupersteps", -1, "Number of supersteps job will run for");

    /**
     * Whether to disable GiraphClassResolver which is an efficient implementation of kryo class
     * resolver. By default this resolver is used by KryoSimpleWritable and KryoSimpleWrapper, and
     * can be disabled with this option
     */
    BooleanConfOption DISABLE_GIRAPH_CLASS_RESOLVER =
            new BooleanConfOption(
                    "giraph.disableGiraphClassResolver",
                    false,
                    "Disables GiraphClassResolver, which is a custom implementation "
                            + "of kryo class resolver that avoids writing class names to the "
                            + "underlying stream for faster serialization.");

    /**
     * Path where jmap exists
     */
    StrConfOption JMAP_PATH =
            new StrConfOption("giraph.jmapPath", "jmap", "Path to use for invoking jmap");

    /**
     * Whether to fail the job or just warn when input is empty
     */
    BooleanConfOption FAIL_ON_EMPTY_INPUT =
            new BooleanConfOption(
                    "giraph.failOnEmptyInput",
                    true,
                    "Whether to fail the job or just warn when input is empty");
}
