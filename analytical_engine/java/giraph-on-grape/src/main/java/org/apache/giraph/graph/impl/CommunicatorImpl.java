package org.apache.giraph.graph.impl;

import org.apache.giraph.graph.Communicator;
import org.apache.hadoop.io.Writable;

/**
 * Default implementation for communicator, which is able to aggregate, broadcast and reduce between
 * Computation instances.
 *
 * The functionality relies on c++ mpi comm. Make sure the comm is avaliable.
 *
 * In our project, Communicator is can only be hold by two class: WorkerContext and AbstractComputation.
 */
public class CommunicatorImpl implements Communicator {

    /**
     * Add a new value
     *
     * @param name  Name of aggregator
     * @param value Value to add
     */
    @Override
    public <A extends Writable> void aggregate(String name, A value) {

    }

    /**
     * Get value of an aggregator.
     *
     * @param name Name of aggregator
     * @return Value of the aggregator
     */
    @Override
    public <A extends Writable> A getAggregatedValue(String name) {
        return null;
    }

    /**
     * Get value broadcasted from master
     *
     * @param name Name of the broadcasted value
     * @return Broadcasted value
     */
    @Override
    public <B extends Writable> B getBroadcast(String name) {
        return null;
    }

    /**
     * Reduce given value.
     *
     * @param name  Name of the reducer
     * @param value Single value to reduce
     */
    @Override
    public void reduce(String name, Object value) {

    }

    /**
     * Reduce given partial value.
     *
     * @param name  Name of the reducer
     * @param value Single value to reduce
     */
    @Override
    public void reduceMerge(String name, Writable value) {

    }
}
