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
package org.apache.giraph.graph.impl;

import com.alibaba.graphscope.communication.FFICommunicator;
import com.alibaba.graphscope.serialization.FFIByteVectorInputStream;
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import com.alibaba.graphscope.stdcxx.FFIByteVectorFactory;
import com.google.common.base.Preconditions;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;

import org.apache.giraph.aggregators.Aggregator;
import org.apache.giraph.comm.WorkerInfo;
import org.apache.giraph.comm.netty.NettyClientV2;
import org.apache.giraph.comm.netty.NettyServer;
import org.apache.giraph.comm.requests.NettyMessage;
import org.apache.giraph.comm.requests.NettyWritableMessage;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.AggregatorManager;
import org.apache.giraph.graph.Communicator;
import org.apache.giraph.master.AggregatorReduceOperation;
import org.apache.giraph.reducers.ReduceOperation;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

public class AggregatorManagerNettyImpl implements AggregatorManager, Communicator {

    private static Logger logger = LoggerFactory.getLogger(AggregatorManagerNettyImpl.class);
    /**
     * Conf
     */
    private final ImmutableClassesGiraphConfiguration<?, ?, ?> conf;

    public CountDownLatch countDownLatch;
    private HashMap<String, AggregatorWrapper<Writable>> aggregators;
    private ByteBufAllocator allocator;
    private NettyClientV2 client;
    private NettyServer server;
    private WorkerInfo workerInfo;
    private int workerId;
    private int workerNum;
    private FFICommunicator communicator;
    private String masterIp;

    public AggregatorManagerNettyImpl(
            final ImmutableClassesGiraphConfiguration<?, ?, ?> conf, int workerId, int workerNum) {
        this.conf = conf;
        this.workerId = workerId;
        this.workerNum = workerNum;
        aggregators = new HashMap<>();
        communicator = null;
        masterIp = null;
        workerInfo = null;
        allocator = new PooledByteBufAllocator();
        countDownLatch = new CountDownLatch(workerNum - 1);
    }

    @Override
    public void init(FFICommunicator communicator) {
        this.communicator = communicator;
        String[] res = getMasterWorkerIp(workerId, workerNum);
        logger.info(String.join(",", res));
        if (workerId == 0) {
            this.workerInfo =
                    new WorkerInfo(
                            workerId, workerNum, res[0], conf.getAggregatorServerInitPort(), res);
            server =
                    new NettyServer(
                            conf,
                            this,
                            workerInfo,
                            new UncaughtExceptionHandler() {
                                @Override
                                public void uncaughtException(Thread t, Throwable e) {
                                    logger.error(t.getId() + ": " + e.toString());
                                }
                            });
            logger.info(
                    "Worker 0 create server success on "
                            + res[0]
                            + ":"
                            + conf.getAggregatorServerInitPort());
        } else {
            this.workerInfo =
                    new WorkerInfo(
                            workerId, workerNum, res[0], conf.getAggregatorServerInitPort(), res);
            client =
                    new NettyClientV2(
                            conf,
                            this,
                            workerInfo,
                            new UncaughtExceptionHandler() {
                                @Override
                                public void uncaughtException(Thread t, Throwable e) {
                                    logger.error(t.getId() + ": " + e.toString());
                                }
                            });
            if (client.isConnected()) {
                logger.info(
                        "Worker "
                                + workerId
                                + " connected to server success on "
                                + res[0]
                                + ":"
                                + conf.getAggregatorServerInitPort());
            } else {
                throw new IllegalStateException("client connection error");
            }
        }
    }

    /**
     * Accept a message from other worker, aggregate to me.
     *
     * @param aggregatorMessage received message.
     */
    @Override
    public void acceptNettyMessage(NettyMessage nettyMessage) {
        if (nettyMessage instanceof NettyWritableMessage) {
            NettyWritableMessage nettyWritableMessage = (NettyWritableMessage) nettyMessage;
            aggregate(nettyWritableMessage.getId(), nettyWritableMessage.getData());
        }
    }

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
     * @param name            aggregator name
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
     * @param name            aggregator name
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
     * @param name  Name of the reducer
     * @param value Single value to reduce
     */
    @Override
    public void reduce(String name, Object value) {}

    /**
     * Reduce given partial value.
     *
     * @param name  Name of the reducer
     * @param value Single value to reduce
     */
    @Override
    public void reduceMerge(String name, Writable value) {}

    /**
     * Set aggregated value. Can be used for initialization or reset.
     *
     * @param name  name for the aggregator
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
     * @param name  a unique name refer to an aggregator
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
     * @param name     Name of the reducer
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
     * @param name               Name of the reducer
     * @param reduceOp           Reduce operations
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
     * @param name  Name of the broadcast object
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

    /**
     * Synchronize aggregator values between workers after superstep.
     */
    @Override
    public void postSuperstep() {
        // for each aggregator, let master node receive all data, then master distribute to all
        // other
        // node. in byte vector.
        for (Entry<String, AggregatorWrapper<Writable>> entry : aggregators.entrySet()) {
            countDownLatch = new CountDownLatch(workerNum - 1);
            String aggregatorKey = entry.getKey();
            Writable value = entry.getValue().getCurrentValue();
            if (value == null) {
                logger.error("aggregator wrapper is null for " + entry.getKey());
                return;
            }
            Preconditions.checkState(value != null);
            if (workerNum <= 1) {
                logger.info("only one worker, skip aggregating..");
                if (!entry.getValue().isPersistent()) {
                    AggregatorWrapper wrapper = entry.getValue();
                    wrapper.setCurrentValue(wrapper.getReduceOp().createInitialValue());
                }
                return;
            }
            if (workerId > 0) {
                if (Objects.isNull(client)) {
                    logger.error("No client found.");
                    return;
                }

                NettyWritableMessage toSend =
                        new NettyWritableMessage(value, 1000000, aggregatorKey);

                logger.info(
                        "Client: "
                                + workerId
                                + "sending aggregator "
                                + aggregatorKey
                                + ", value: "
                                + toSend.toString()
                                + " "
                                + toSend.getSerializedSize());

                Future<NettyMessage> response = client.sendMessage(toSend);

                NettyMessage received = client.getResponse();
                logger.info("client received msg: " + received);
                if (received instanceof NettyWritableMessage) {
                    NettyWritableMessage nettyWritableMessage = (NettyWritableMessage) received;
                    entry.getValue().setCurrentValue(nettyWritableMessage.getData());
                } else {
                    logger.error("client: [" + workerId + "] received not nettyWritableMessage");
                }

                // decode
                logger.info("worker: [" + workerId + "] finish post super step");
            } else {
                logger.info(
                        "Server "
                                + Thread.currentThread().getId()
                                + "wait for worker request for aggregator: "
                                + aggregatorKey);
                // while (!server.gotEnoughRequestForOneAgg()) {}
                synchronized (server.getMsgNo()) {
                    try {
                        logger.info("server try wait on " + server.getMsgNo());
                        while (server.getMsgNo().get() == 0
                                || (server.getMsgNo().get() % (workerNum - 1)) != 0) {
                            server.getMsgNo().wait();
                        }
                        logger.info("server finish waiting on " + server.getMsgNo());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                logger.info("netty server got enough requests for one agg, proceed..");
                logger.info("server: [" + workerId + "] finish post super step");
            }
        }
    }

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

    private String[] getMasterWorkerIp(int fid, int fnum) {
        if (Objects.isNull(communicator)) {
            logger.error("Please set communicator first");
            return null;
        }
        String selfIp = "";
        try {
            selfIp = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            logger.error("Failed to get master host address");
            return null;
        }

        if (fid == 0) {
            logger.info("Master host: " + selfIp);
            FFIByteVectorOutputStream outputStream = new FFIByteVectorOutputStream();
            try {
                outputStream.writeUTF(selfIp);
            } catch (IOException e) {
                e.printStackTrace();
                logger.info("Error in writing output.");
                return null;
            }
            outputStream.finishSetting();
            // Send what received to all worker
            for (int dstWroker = 1; dstWroker < fnum; ++dstWroker) {
                communicator.sendTo(dstWroker, outputStream.getVector());
            }
            logger.info("master finish sending");
            // Receive slaves' ips.
            FFIByteVectorInputStream inputStream = new FFIByteVectorInputStream();
            for (int srcWorker = 1; srcWorker < fnum; ++srcWorker) {
                FFIByteVector vector = (FFIByteVector) FFIByteVectorFactory.INSTANCE.create();
                communicator.receiveFrom(srcWorker, vector);
                inputStream.digestVector(vector);
            }
            String[] res = new String[fnum];
            res[0] = selfIp;
            try {
                for (int i = 1; i < fnum; ++i) {
                    res[i] = inputStream.readUTF();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            logger.info("Master got all ips: " + String.join(",", res));
            return res;
        } else {
            String coordinatorIp = null;
            FFIByteVectorInputStream inputStream = new FFIByteVectorInputStream();
            FFIByteVector vector = (FFIByteVector) FFIByteVectorFactory.INSTANCE.create();
            communicator.receiveFrom(0, vector);
            inputStream.digestVector(vector);
            logger.info(
                    "worker: "
                            + workerId
                            + " receive size: "
                            + inputStream.longAvailable()
                            + " from worker 0");
            try {
                coordinatorIp = inputStream.readUTF();
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (inputStream.longAvailable() > 0) {
                logger.error("Still has message after read utf8");
            }
            FFIByteVectorOutputStream outputStream = new FFIByteVectorOutputStream();
            try {
                outputStream.writeUTF(selfIp);
            } catch (Exception e) {
                e.printStackTrace();
            }
            outputStream.finishSetting();
            communicator.sendTo(0, outputStream.getVector());
            logger.info("worker[" + workerId + "] finish sending");
            return new String[] {coordinatorIp};
        }
    }

    private class AggregatorWrapper<A extends Writable> implements Writable {

        /**
         * False iff aggregator should be reset at the end of each super step
         */
        private boolean persistent;
        /**
         * Translation of aggregator to reduce operations
         */
        private AggregatorReduceOperation<A> reduceOp;
        /**
         * Current value, set by master manually
         */
        private A currentValue;

        /**
         * Constructor
         */
        public AggregatorWrapper() {}

        /**
         * Constructor
         *
         * @param aggregatorClass Aggregator class
         * @param persistent      Is persistent
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
