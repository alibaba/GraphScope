/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.utils;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import org.apache.giraph.conf.FloatConfOption;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.IntConfOption;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Wraps a list of byte array outputs and provides convenient utilities on top of it
 */
public class ExtendedByteArrayOutputBuffer {

    /**
     * This option sets the capacity of an {@link ExtendedDataOutput} instance created in {@link
     * ExtendedByteArrayOutputBuffer}
     */
    public static final IntConfOption CAPACITY_OF_DATAOUT_IN_BUFFER =
            new IntConfOption(
                    "giraph.capacityOfDataOutInBuffer",
                    1024 * GiraphConstants.ONE_KB,
                    "Set the capacity of dataoutputs in dataout buffer");

    /**
     * This option sets the maximum fraction of a {@link ExtendedDataOutput} instance (stored in
     * {@link ExtendedByteArrayOutputBuffer}) that can be filled
     */
    public static final FloatConfOption FILLING_THRESHOLD_OF_DATAOUT_IN_BUFFER =
            new FloatConfOption(
                    "giraph.fillingThresholdOfDataoutInBuffer",
                    0.98f,
                    "Set the maximum fraction of dataoutput capacity allowed to fill");

    /**
     * Maximum size allowed for one byte array output
     */
    private final int maxBufSize;
    /**
     * Stop writing to buffer after threshold has been reached
     */
    private final int threshold;
    /**
     * Giraph configuration
     */
    private final ImmutableClassesGiraphConfiguration<?, ?, ?> config;

    /**
     * Map of index => byte array outputs
     */
    private final Int2ObjectOpenHashMap<ExtendedDataOutput> bytearrayOutputs =
            new Int2ObjectOpenHashMap<>();
    /**
     * Size of byte array outputs map
     */
    private final AtomicInteger mapSize = new AtomicInteger(0);
    /**
     * Thread local variable to get hold of a byte array output stream
     */
    private final ThreadLocal<IndexAndDataOut> threadLocal =
            new ThreadLocal<IndexAndDataOut>() {
                @Override
                protected IndexAndDataOut initialValue() {
                    return newIndexAndDataOutput();
                }
            };

    /**
     * Constructor
     *
     * @param config configuration
     */
    public ExtendedByteArrayOutputBuffer(ImmutableClassesGiraphConfiguration<?, ?, ?> config) {
        this.config = config;

        maxBufSize = CAPACITY_OF_DATAOUT_IN_BUFFER.get(config);
        threshold = (int) (FILLING_THRESHOLD_OF_DATAOUT_IN_BUFFER.get(config) * maxBufSize);
    }

    /**
     * Return threadLocal indexAndDataOutput instance
     *
     * @return threadLocal indexAndDataOutput instance
     */
    public IndexAndDataOut getIndexAndDataOut() {
        IndexAndDataOut indexAndDataOut = threadLocal.get();
        if (indexAndDataOut.dataOutput.getPos() >= threshold) {
            indexAndDataOut = newIndexAndDataOutput();
            threadLocal.set(indexAndDataOut);
        }
        return indexAndDataOut;
    }

    /**
     * Get dataoutput from bytearrayOutputs
     *
     * @param index index in bytearrayOutputs
     * @return extendeddataoutput at given index
     */
    public ExtendedDataOutput getDataOutput(int index) {
        return bytearrayOutputs.get(index);
    }

    /**
     * Create a new IndexAndDataOutput instance
     *
     * @return new IndexAndDataOutput instance
     */
    private IndexAndDataOut newIndexAndDataOutput() {
        int index = mapSize.getAndIncrement();
        ExtendedDataOutput output = config.createExtendedDataOutput(maxBufSize);
        synchronized (bytearrayOutputs) {
            bytearrayOutputs.put(index, output);
        }
        return new IndexAndDataOut(index, output);
    }

    /**
     * Holder for index &amp; DataOutput objects
     */
    public static class IndexAndDataOut {

        /**
         * Index
         */
        private final int index;
        /**
         * Dataouput instance
         */
        private final ExtendedDataOutput dataOutput;

        /**
         * Constructor
         *
         * @param index      index in bytearrayOutputs
         * @param dataOutput dataoutput
         */
        public IndexAndDataOut(int index, ExtendedDataOutput dataOutput) {
            this.index = index;
            this.dataOutput = dataOutput;
        }

        public int getIndex() {
            return index;
        }

        public ExtendedDataOutput getDataOutput() {
            return dataOutput;
        }
    }
}
