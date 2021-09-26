package org.apache.giraph.graph;

import com.alibaba.graphscope.communication.FFICommunicator;

import org.apache.giraph.aggregators.Aggregator;
import org.apache.giraph.comm.requests.NettyMessage;
import org.apache.giraph.reducers.ReduceOperation;
import org.apache.hadoop.io.Writable;

/** Providing management for creating and using aggregators. */
public interface AggregatorManager {

    int getWorkerId();

    int getNumWorkers();

    /**
     * Register an aggregator with a unique name
     *
     * @param name aggregator name
     * @param aggregatorClass the class
     * @param <A> type param
     */
    <A extends Writable> boolean registerAggregator(
            String name, Class<? extends Aggregator<A>> aggregatorClass)
            throws InstantiationException, IllegalAccessException;

    /**
     * Register a persistent aggregator with a unique name.
     *
     * @param name aggregator name
     * @param aggregatorClass the implementation class
     * @param <A> type param
     */
    <A extends Writable> boolean registerPersistentAggregator(
            String name, Class<? extends Aggregator<A>> aggregatorClass)
            throws InstantiationException, IllegalAccessException;

    /**
     * Return current aggregated value. Needs to be initialized if aggregate or setAggregatedValue
     * have not been called before.
     *
     * @param name name for the aggregator
     * @return Aggregated
     */
    <A extends Writable> A getAggregatedValue(String name);

    /**
     * Set aggregated value. Can be used for initialization or reset.
     *
     * @param name name for the aggregator
     * @param value Value to be set.
     */
    <A extends Writable> void setAggregatedValue(String name, A value);

    /**
     * Add a new value. Needs to be commutative and associative
     *
     * @param name a unique name refer to an aggregator
     * @param value Value to be aggregated.
     */
    <A extends Writable> void aggregate(String name, A value);

    /**
     * Register reducer to be reduced in the next worker computation, using given name and
     * operations.
     *
     * @param name Name of the reducer
     * @param reduceOp Reduce operations
     * @param <S> Single value type
     * @param <R> Reduced value type
     */
    <S, R extends Writable> void registerReducer(String name, ReduceOperation<S, R> reduceOp);

    /**
     * Register reducer to be reduced in the next worker computation, using given name and
     * operations, starting globally from globalInitialValue. (globalInitialValue is reduced only
     * once, each worker will still start from neutral initial value)
     *
     * @param name Name of the reducer
     * @param reduceOp Reduce operations
     * @param globalInitialValue Global initial value
     * @param <S> Single value type
     * @param <R> Reduced value type
     */
    <S, R extends Writable> void registerReducer(
            String name, ReduceOperation<S, R> reduceOp, R globalInitialValue);

    /**
     * Get reduced value from previous worker computation.
     *
     * @param name Name of the reducer
     * @param <R> Reduced value type
     * @return Reduced value
     */
    <R extends Writable> R getReduced(String name);

    /**
     * Broadcast given value to all workers for next computation.
     *
     * @param name Name of the broadcast object
     * @param value Value
     */
    void broadcast(String name, Writable value);

    void preSuperstep();

    /** Synchronize aggregator values between workers after superstep. */
    void postSuperstep();

    /**
     * Init the manager with Grape::Communicator, the actual logic depends on implementation.
     *
     * @param communicator communicator.
     */
    void init(FFICommunicator communicator);

    /**
     * Accept a message from other worker, aggregate to me.
     *
     * @param aggregatorMessage received message.
     */
    void acceptNettyMessage(NettyMessage aggregatorMessage);
}
