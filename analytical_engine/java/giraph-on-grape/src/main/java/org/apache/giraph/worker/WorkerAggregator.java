package org.apache.giraph.worker;

import org.apache.hadoop.io.Writable;

public interface WorkerAggregator {

    void reduce(String name, Object value);

    void reduceMerge(String name, Writable value);

    <B extends Writable> B getBroadcast(String name);

    <A extends Writable> void aggregate(String name, A value);

    <A extends Writable> A getAggregatedValue(String name);
}
