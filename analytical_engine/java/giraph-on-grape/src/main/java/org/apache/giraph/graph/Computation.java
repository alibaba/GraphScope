package org.apache.giraph.graph;

import java.io.IOException;
import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.TypesHolder;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerIndexUsage;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * We redefine the Computation interface in our SDK, keep the api methods and signature the same,
 * but we will substitute all interfaces with our ones.
 * @param <OID_T> vertex original id type
 * @param <VDATA_T> vertex data type
 * @param <EDATA_T> edge data type
 * @param <IN_MSG_T> incoming msg type
 * @param <OUT_MSG_T> outgoing msg type
 */
public interface Computation<OID_T extends WritableComparable,
    VDATA_T extends Writable,
    EDATA_T extends Writable,
    IN_MSG_T extends Writable,
    OUT_MSG_T extends Writable>
    extends Communicator, GraphManager<OID_T,VDATA_T,EDATA_T>, MessageSender<OID_T,VDATA_T,EDATA_T,OUT_MSG_T>, WorkerIndexUsage<OID_T>,
    TypesHolder<OID_T,VDATA_T,EDATA_T,IN_MSG_T,OUT_MSG_T>,
    ImmutableClassesGiraphConfigurable<OID_T, VDATA_T, EDATA_T> {

    /**
     * Must be defined by user to do computation on a single Vertex.
     *
     * @param vertex   Vertex
     * @param messages Messages that were sent to this vertex in the previous
     *                 superstep.  Each message is only guaranteed to have
     *                 a life expectancy as long as next() is not called.
     */
    void compute(Vertex<OID_T, VDATA_T, EDATA_T> vertex, Iterable<IN_MSG_T> messages)
        throws IOException;

    /**
     * Prepare for computation. This method is executed exactly once prior to
     * {@link #compute(Vertex, Iterable)} being called for any of the vertices
     * in the partition.
     */
    void preSuperstep();

    /**
     * Finish computation. This method is executed exactly once after computation
     * for all vertices in the partition is complete.
     */
    void postSuperstep();

    /**
     * Retrieves the current superstep.
     *
     * @return Current superstep
     */
    long getSuperstep();

    /**
     * Get the total (all workers) number of vertices that
     * existed in the previous superstep.
     *
     * @return Total number of vertices (-1 if first superstep)
     */
    long getTotalNumVertices();

    /**
     * Get the total (all workers) number of edges that
     * existed in the previous superstep.
     *
     * @return Total number of edges (-1 if first superstep)
     */
    long getTotalNumEdges();

    /**
     * Get the mapper context
     *
     * @return Mapper context
     */
    Mapper.Context getContext();

    /**
     * Get the worker context
     *
     * @param <W> WorkerContext class
     * @return WorkerContext context
     */
    @SuppressWarnings("unchecked")
    <W extends WorkerContext> W getWorkerContext();
}
