package org.apache.giraph.graph;

import org.apache.hadoop.io.Writable;

/**
 * Define the interface for a global communicator. Implementations should take care of all fragment
 * inside aggregation and between-fragment aggregation.
 */
public interface Communicator {
    /**
     * Add a new value
     *
     * @param name Name of aggregator
     * @param value Value to add
     * @param <A> Aggregated value
     */
    <A extends Writable> void aggregate(String name, A value);

    /**
     * Get value of an aggregator.
     *
     * @param name Name of aggregator
     * @param <A> Aggregated value
     * @return Value of the aggregator
     */
    <A extends Writable> A getAggregatedValue(String name);

    /**
     * Get value broadcasted from master
     * @param name Name of the broadcasted value
     * @return Broadcasted value
     * @param <B> Broadcast value type
     */
    <B extends Writable> B getBroadcast(String name);

    /**
     * Reduce given value.
     * @param name Name of the reducer
     * @param value Single value to reduce
     */
    void reduce(String name, Object value);

    /**
     * Reduce given partial value.
     * @param name Name of the reducer
     * @param value Single value to reduce
     */
    void reduceMerge(String name, Writable value);
}
