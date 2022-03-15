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

package org.apache.giraph.master;

import com.google.common.base.Preconditions;

import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.conf.DefaultMessageClasses;
import org.apache.giraph.conf.GiraphClasses;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.MessageClasses;
import org.apache.giraph.conf.TypesHolder;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Holds Computation and MessageCombiner class.
 */
public class SuperstepClasses implements Writable {

    /**
     * Class logger
     */
    private static final Logger logger = LoggerFactory.getLogger(SuperstepClasses.class);
    /**
     * Configuration
     */
    private final ImmutableClassesGiraphConfiguration conf;

    /**
     * Computation class to be used in the following superstep
     */
    private Class<? extends Computation> computationClass;
    /**
     * Incoming message classes, immutable, only here for cheecking
     */
    private MessageClasses<? extends WritableComparable, ? extends Writable> incomingMessageClasses;
    /**
     * Outgoing message classes
     */
    private MessageClasses<? extends WritableComparable, ? extends Writable> outgoingMessageClasses;

    /**
     * Constructor
     *
     * @param conf                   Configuration
     * @param computationClass       computation class
     * @param incomingMessageClasses incoming message classes
     * @param outgoingMessageClasses outgoing message classes
     */
    public SuperstepClasses(
            ImmutableClassesGiraphConfiguration conf,
            Class<? extends Computation> computationClass,
            MessageClasses<? extends WritableComparable, ? extends Writable> incomingMessageClasses,
            MessageClasses<? extends WritableComparable, ? extends Writable>
                    outgoingMessageClasses) {
        this.conf = conf;
        this.computationClass = computationClass;
        this.incomingMessageClasses = incomingMessageClasses;
        this.outgoingMessageClasses = outgoingMessageClasses;
    }

    /**
     * Create empty superstep classes, readFields needs to be called afterwards
     *
     * @param conf Configuration
     * @return Superstep classes
     */
    public static SuperstepClasses createToRead(ImmutableClassesGiraphConfiguration conf) {
        return new SuperstepClasses(conf, null, null, null);
    }

    /**
     * Create superstep classes by initiazling from current state in configuration
     *
     * @param conf Configuration
     * @return Superstep classes
     */
    public static SuperstepClasses createAndExtractTypes(ImmutableClassesGiraphConfiguration conf) {
        return new SuperstepClasses(
                conf,
                conf.getComputationClass(),
                conf.getOutgoingMessageClasses(),
                conf.getOutgoingMessageClasses().createCopyForNewSuperstep());
    }

    public Class<? extends Computation> getComputationClass() {
        return computationClass;
    }

    /**
     * Set computation class
     *
     * @param computationClass computation class
     */
    public void setComputationClass(Class<? extends Computation> computationClass) {
        this.computationClass = computationClass;

        if (computationClass != null) {
            Class[] computationTypes =
                    ReflectionUtils.getTypeArguments(TypesHolder.class, computationClass);
            if (computationTypes[4] != null
                    && outgoingMessageClasses instanceof DefaultMessageClasses) {
                ((DefaultMessageClasses) outgoingMessageClasses)
                        .setIfNotModifiedMessageClass(computationTypes[4]);
            }
        }
    }

    public MessageClasses<? extends WritableComparable, ? extends Writable>
            getOutgoingMessageClasses() {
        return outgoingMessageClasses;
    }

    /**
     * Set's outgoing MessageClasses for next superstep. Should not be used together with
     * setMessageCombinerClass/setOutgoingMessageClass methods.
     *
     * @param outgoingMessageClasses outgoing message classes
     */
    public void setOutgoingMessageClasses(
            MessageClasses<? extends WritableComparable, ? extends Writable>
                    outgoingMessageClasses) {
        this.outgoingMessageClasses = outgoingMessageClasses;
    }

    /**
     * Set incoming message class
     *
     * @param incomingMessageClass incoming message class
     */
    @Deprecated
    public void setIncomingMessageClass(Class<? extends Writable> incomingMessageClass) {
        if (!incomingMessageClasses.getMessageClass().equals(incomingMessageClass)) {
            throw new IllegalArgumentException(
                    "Cannot change incoming message class from "
                            + incomingMessageClasses.getMessageClass()
                            + " previously, to "
                            + incomingMessageClass);
        }
    }

    /**
     * Set outgoing message class. Should not be used together setOutgoingMessageClasses (throws
     * exception if called with it), as it is unnecessary to do so.
     *
     * @param outgoingMessageClass outgoing message class
     */
    public void setOutgoingMessageClass(Class<? extends Writable> outgoingMessageClass) {
        Preconditions.checkState(outgoingMessageClasses instanceof DefaultMessageClasses);
        ((DefaultMessageClasses) outgoingMessageClasses).setMessageClass(outgoingMessageClass);
    }

    /**
     * Get message combiner class
     *
     * @return message combiner class
     */
    public Class<? extends MessageCombiner> getMessageCombinerClass() {
        MessageCombiner combiner = outgoingMessageClasses.createMessageCombiner(conf);
        return combiner != null ? combiner.getClass() : null;
    }

    /**
     * Set message combiner class. Should not be used together setOutgoingMessageClasses (throws
     * exception if called with it), as it is unnecessary to do so.
     *
     * @param messageCombinerClass message combiner class
     */
    public void setMessageCombinerClass(Class<? extends MessageCombiner> messageCombinerClass) {
        Preconditions.checkState(outgoingMessageClasses instanceof DefaultMessageClasses);
        ((DefaultMessageClasses) outgoingMessageClasses)
                .setMessageCombinerClass(messageCombinerClass);
    }

    /**
     * Verify that types of current Computation and MessageCombiner are valid. If types don't match
     * an {@link IllegalStateException} will be thrown.
     *
     * @param checkMatchingMesssageTypes Check that the incoming/outgoing message types match
     */
    public void verifyTypesMatch(boolean checkMatchingMesssageTypes) {

        Class<?>[] computationTypes =
                ReflectionUtils.getTypeArguments(TypesHolder.class, computationClass);
        ReflectionUtils.verifyTypes(
                conf.getVertexIdClass(), computationTypes[0], "Vertex id", computationClass);
        ReflectionUtils.verifyTypes(
                conf.getVertexValueClass(), computationTypes[1], "Vertex value", computationClass);
        ReflectionUtils.verifyTypes(
                conf.getEdgeValueClass(), computationTypes[2], "Edge value", computationClass);

        if (checkMatchingMesssageTypes) {
            ReflectionUtils.verifyTypes(
                    incomingMessageClasses.getMessageClass(),
                    computationTypes[3],
                    "Incoming message type",
                    computationClass);
        }

        ReflectionUtils.verifyTypes(
                outgoingMessageClasses.getMessageClass(),
                computationTypes[4],
                "Outgoing message type",
                computationClass);

        outgoingMessageClasses.verifyConsistent(conf);
    }

    /**
     * Update GiraphClasses with updated types
     *
     * @param classes Giraph classes
     */
    public void updateGiraphClasses(GiraphClasses classes) {
        classes.setComputationClass(computationClass);
        classes.setIncomingMessageClasses(incomingMessageClasses);
        classes.setOutgoingMessageClasses(outgoingMessageClasses);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        WritableUtils.writeClass(computationClass, output);
        WritableUtils.writeWritableObject(incomingMessageClasses, output);
        WritableUtils.writeWritableObject(outgoingMessageClasses, output);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        computationClass = WritableUtils.readClass(input);
        incomingMessageClasses = WritableUtils.readWritableObject(input, conf);
        outgoingMessageClasses = WritableUtils.readWritableObject(input, conf);
    }

    @Override
    public String toString() {
        String computationName =
                computationClass == null ? "_not_set_" : computationClass.getName();
        return "(computation="
                + computationName
                + ",incoming="
                + incomingMessageClasses
                + ",outgoing="
                + outgoingMessageClasses
                + ")";
    }
}
