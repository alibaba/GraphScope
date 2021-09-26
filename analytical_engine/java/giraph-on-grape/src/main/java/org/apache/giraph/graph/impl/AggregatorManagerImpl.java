package org.apache.giraph.graph.impl;

import com.alibaba.graphscope.communication.FFICommunicator;
import com.alibaba.graphscope.serialization.FFIByteVectorInputStream;
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import com.alibaba.graphscope.stdcxx.FFIByteVectorFactory;
import com.google.common.base.Preconditions;

import org.apache.giraph.aggregators.Aggregator;
import org.apache.giraph.comm.requests.NettyMessage;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.AggregatorManager;
import org.apache.giraph.graph.Communicator;
import org.apache.giraph.master.AggregatorReduceOperation;
import org.apache.giraph.reducers.ReduceOperation;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Objects;

public class AggregatorManagerImpl implements AggregatorManager, Communicator {

    private static Logger logger = LoggerFactory.getLogger(AggregatorManagerImpl.class);

    private HashMap<String, AggregatorWrapper<Writable>> aggregators;
    //    private HashMap<String,Aggregator> unPersistentAggregators;
    /** Conf */
    private final ImmutableClassesGiraphConfiguration<?, ?, ?> conf;

    private int workerId;
    private int workerNum;

    /** Java wrapper for grape mpi comm */
    private FFICommunicator communicator;
    /** after aggregation, all data will be available in this stream */
    FFIByteVectorInputStream inputStream = new FFIByteVectorInputStream();
    /** a temp stream for us to write data into byte array */
    FFIByteVectorOutputStream outputStream = new FFIByteVectorOutputStream();
    /** vector to receive mpi message */
    FFIByteVector received = (FFIByteVector) FFIByteVectorFactory.INSTANCE.create();

    public AggregatorManagerImpl(
            ImmutableClassesGiraphConfiguration<?, ?, ?> conf, int workerId, int workerNum) {
        this.conf = conf;
        aggregators = new HashMap<>();
        this.workerId = workerId;
        this.workerNum = workerNum;
    }

    @Override
    public void init(FFICommunicator communicator) {
        this.communicator = communicator;
    }

    /**
     * Accept a message from other worker, aggregate to me.
     *
     * @param aggregatorMessage received message.
     */
    @Override
    public void acceptNettyMessage(NettyMessage aggregatorMessage) {}

    @Override
    public int getWorkerId() {
        return workerId;
    }

    @Override
    public int getNumWorkers() {
        return workerNum;
    }

    /**
     * Register an aggregator with a unique name
     *
     * @param name aggregator name
     * @param aggregatorClass the class
     */
    @Override
    public <A extends Writable> boolean registerAggregator(
            String name, Class<? extends Aggregator<A>> aggregatorClass)
            throws InstantiationException, IllegalAccessException {
        return registerAggregator(name, aggregatorClass, false);
    }

    /**
     * Register a persistent aggregator with a unique name.
     *
     * @param name aggregator name
     * @param aggregatorClass the implementation class
     */
    @Override
    public <A extends Writable> boolean registerPersistentAggregator(
            String name, Class<? extends Aggregator<A>> aggregatorClass)
            throws InstantiationException, IllegalAccessException {
        return registerAggregator(name, aggregatorClass, true);
    }

    /**
     * Return current aggregated value. Needs to be initialized if aggregate or setAggregatedValue
     * have not been called before.
     *
     * @param name name for the aggregator
     * @return Aggregated
     */
    @Override
    public <A extends Writable> A getAggregatedValue(String name) {
        AggregatorWrapper<Writable> agg = aggregators.get(name);
        if (agg == null) {
            logger.error("No such aggregator " + name);
            // to make sure we are not accessing reducer of the same name.
            return null;
        }
        return (A) agg.getCurrentValue();
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
     * @param name Name of the reducer
     * @param value Single value to reduce
     */
    @Override
    public void reduce(String name, Object value) {}

    /**
     * Reduce given partial value.
     *
     * @param name Name of the reducer
     * @param value Single value to reduce
     */
    @Override
    public void reduceMerge(String name, Writable value) {}

    /**
     * Set aggregated value. Can be used for initialization or reset.
     *
     * @param name name for the aggregator
     * @param value Value to be set.
     */
    @Override
    public <A extends Writable> void setAggregatedValue(String name, A value) {
        AggregatorWrapper<Writable> agg = aggregators.get(name);
        if (agg == null) {
            logger.error("No such aggregator " + name);
            // to make sure we are not accessing reducer of the same name.
            return;
        }
        agg.setCurrentValue(value);
    }

    /**
     * Add a new value. Needs to be commutative and associative
     *
     * @param name a unique name refer to an aggregator
     * @param value Value to be aggregated.
     */
    @Override
    public <A extends Writable> void aggregate(String name, A value) {
        AggregatorWrapper<A> aggregatorWrapper = (AggregatorWrapper<A>) aggregators.get(name);
        if (Objects.isNull(aggregatorWrapper)) {
            logger.error("No-existing aggregator: " + name);
            return;
        }
        aggregatorWrapper.reduce(value);
    }

    /**
     * Register reducer to be reduced in the next worker computation, using given name and
     * operations.
     *
     * @param name Name of the reducer
     * @param reduceOp Reduce operations
     */
    @Override
    public <S, R extends Writable> void registerReducer(
            String name, ReduceOperation<S, R> reduceOp) {}

    /**
     * Register reducer to be reduced in the next worker computation, using given name and
     * operations, starting globally from globalInitialValue. (globalInitialValue is reduced only
     * once, each worker will still start from neutral initial value)
     *
     * @param name Name of the reducer
     * @param reduceOp Reduce operations
     * @param globalInitialValue Global initial value
     */
    @Override
    public <S, R extends Writable> void registerReducer(
            String name, ReduceOperation<S, R> reduceOp, R globalInitialValue) {}

    /**
     * Get reduced value from previous worker computation.
     *
     * @param name Name of the reducer
     * @return Reduced value
     */
    @Override
    public <R extends Writable> R getReduced(String name) {
        return null;
    }

    /**
     * Broadcast given value to all workers for next computation.
     *
     * @param name Name of the broadcast object
     * @param value Value
     */
    @Override
    public void broadcast(String name, Writable value) {}

    @Override
    public void preSuperstep() {
        for (Entry<String, AggregatorWrapper<Writable>> entry : aggregators.entrySet()) {
            if (!entry.getValue().isPersistent()) {
                logger.info(
                        "Aggregator: "
                                + entry.getKey()
                                + " is not persistent, reset before superstep");
                entry.getValue()
                        .setCurrentValue(entry.getValue().getReduceOp().createInitialValue());
            }
        }
    }

    /** Synchronize aggregator values between workers after superstep. */
    @Override
    public void postSuperstep() {
        // for each aggregator, let master node receive all data, then master distribute to all
        // other
        // node. in byte vector.
        for (Entry<String, AggregatorWrapper<Writable>> entry : aggregators.entrySet()) {
            String aggregatorKey = entry.getKey();
            Writable value = entry.getValue().getCurrentValue();
            if (value == null) {
                logger.error("aggregator wrapper is null for " + entry.getKey());
                return;
            }
            Preconditions.checkState(value != null);

            outputStream.reset();
            inputStream.clear();

            try {
                if (workerNum > 1) {
                    value.write(outputStream);
                    outputStream.finishSetting();
                    if (workerId == 0) {
                        inputStream.digestVector(outputStream.getVector());
                        for (int src_worker = 1; src_worker < workerNum; ++src_worker) {
                            communicator.receiveFrom(src_worker, received);
                            logger.info(
                                    "Receive from src_worker: "
                                            + src_worker
                                            + ", size : "
                                            + received.size());
                            inputStream.digestVector(received);
                        }

                        // Reset
                        AggregatorWrapper wrapper = entry.getValue();
                        wrapper.setCurrentValue(wrapper.getReduceOp().createInitialValue());

                        // digest input stream
                        logger.info(
                                "master: "
                                        + workerId
                                        + " aggregator: "
                                        + aggregatorKey
                                        + " ,receive msg: "
                                        + inputStream.longAvailable());
                        Writable msg =
                                ReflectionUtils.newInstance(wrapper.getCurrentValue().getClass());
                        logger.info("Parse aggregation msg in " + msg.getClass().getName());
                        // parse to writables
                        while (inputStream.longAvailable() > 0) {
                            msg.readFields(inputStream);
                            // apply aggregator on received writables.
                            wrapper.reduce(msg);
                            logger.info(
                                    "worker: "
                                            + workerId
                                            + "aggregator: "
                                            + aggregatorKey
                                            + " reduce: "
                                            + msg
                                            + ", to"
                                            + wrapper.getCurrentValue());
                        }
                        logger.info(
                                "server: "
                                        + workerId
                                        + "aggregator: "
                                        + aggregatorKey
                                        + " after aggregation: "
                                        + wrapper.getCurrentValue());
                        // Wrap result in output stream
                        outputStream.reset();
                        wrapper.currentValue.write(outputStream);

                        // Send what received to all worker
                        for (int dstWroker = 1; dstWroker < workerNum; ++dstWroker) {
                            communicator.sendTo(dstWroker, outputStream.getVector());
                        }
                    } else {
                        logger.info(
                                "worker: "
                                        + workerId
                                        + " sending size: "
                                        + outputStream.getVector().size()
                                        + " to worker 0");
                        communicator.sendTo(0, outputStream.getVector());

                        communicator.receiveFrom(0, received);
                        inputStream.digestVector(received);
                        logger.info(
                                "worker: "
                                        + workerId
                                        + " receive size: "
                                        + inputStream.longAvailable()
                                        + " from worker 0");
                        AggregatorWrapper wrapper = entry.getValue();
                        Writable msg =
                                ReflectionUtils.newInstance(wrapper.getCurrentValue().getClass());
                        msg.readFields(inputStream);
                        logger.info("Worker " + workerId + " ] received final value: " + msg);
                        wrapper.setCurrentValue(msg);
                    }
                } else {
                    logger.info("only one worker, skip aggregating..");
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // TODO: figure out how this works
    public void postMasterCompute() {
        // broadcast what master set, or if it didn't broadcast reduced value
        // register reduce with the same value
        for (Entry<String, AggregatorWrapper<Writable>> entry : aggregators.entrySet()) {
            Writable value = entry.getValue().getCurrentValue();
            if (value == null) {
                logger.error("aggregator wrapper is null for " + entry.getKey());
                return;
            }
            Preconditions.checkState(value != null);

            // Synchronize the value

            // For persistent aggregators, we do nothing
            // For non persistent ones, we reset the initial value.
            AggregatorReduceOperation<Writable> cleanReduceOp = entry.getValue().createReduceOp();
            if (entry.getValue().isPersistent()) {
                logger.info("Aggregator: " + entry.getKey() + " is persistent.");
            } else {
                logger.info("Aggregator: " + entry.getKey() + " is non-persistent");
                entry.getValue()
                        .setCurrentValue(entry.getValue().getReduceOp().createInitialValue());
            }
            //            entry.getValue().setCurrentValue(null);
        }
        //        initAggregatorValues.clear();
    }

    private <A extends Writable> boolean registerAggregator(
            String name, Class<? extends Aggregator<A>> aggregatorClass, boolean persistent) {
        if (aggregators.containsKey(name)) {
            logger.error("Name: " + name + " has already been registered " + aggregators.get(name));
            return false;
        }
        AggregatorWrapper<A> aggregatorWrapper = (AggregatorWrapper<A>) aggregators.get(name);
        aggregatorWrapper = new AggregatorWrapper<A>(aggregatorClass, persistent);
        // postMasterCompute uses previously reduced value to broadcast,
        // unless current value is set. After aggregator is registered,
        // there was no previously reduced value, so set current value
        // to default to avoid calling getReduced() on unregistered reducer.
        // (which logs unnecessary warnings)
        aggregatorWrapper.setCurrentValue(aggregatorWrapper.getReduceOp().createInitialValue());
        aggregators.put(name, (AggregatorWrapper<Writable>) aggregatorWrapper);
        return true;
    }

    private class AggregatorWrapper<A extends Writable> implements Writable {

        /** False iff aggregator should be reset at the end of each super step */
        private boolean persistent;
        /** Translation of aggregator to reduce operations */
        private AggregatorReduceOperation<A> reduceOp;
        /** Current value, set by master manually */
        private A currentValue;

        /** Constructor */
        public AggregatorWrapper() {}

        /**
         * Constructor
         *
         * @param aggregatorClass Aggregator class
         * @param persistent Is persistent
         */
        public AggregatorWrapper(
                Class<? extends Aggregator<A>> aggregatorClass, boolean persistent) {
            this.persistent = persistent;
            this.reduceOp = new AggregatorReduceOperation<>(aggregatorClass, conf);
        }

        public AggregatorReduceOperation<A> getReduceOp() {
            return reduceOp;
        }

        /**
         * Create a fresh instance of AggregatorReduceOperation
         *
         * @return fresh instance of AggregatorReduceOperation
         */
        public AggregatorReduceOperation<A> createReduceOp() {
            return reduceOp.createCopy();
        }

        public A getCurrentValue() {
            return currentValue;
        }

        public void setCurrentValue(A currentValue) {
            this.currentValue = currentValue;
        }

        public boolean isPersistent() {
            return persistent;
        }

        public void reduce(A value) {
            logger.info("Before reduce: " + currentValue + ", " + value);
            currentValue = reduceOp.reduce(currentValue, value);
            logger.info("After reduce: " + currentValue);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeBoolean(persistent);
            reduceOp.write(out);

            Preconditions.checkState(
                    currentValue == null,
                    "AggregatorWrapper " + "shouldn't have value at the end of the superstep");
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            persistent = in.readBoolean();
            reduceOp = WritableUtils.createWritable(AggregatorReduceOperation.class, conf);
            reduceOp.readFields(in);
            currentValue = null;
        }
    }
}
