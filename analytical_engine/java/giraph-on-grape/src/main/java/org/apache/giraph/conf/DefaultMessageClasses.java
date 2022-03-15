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
package org.apache.giraph.conf;

import com.alibaba.graphscope.parallel.message.MessageEncodeAndStoreType;
import com.google.common.base.Preconditions;

import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.factories.DefaultMessageValueFactory;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Default implementation of MessageClasses
 *
 * @param <I> Vertex id type
 * @param <M> Message type
 */
public class DefaultMessageClasses<I extends WritableComparable, M extends Writable>
        implements MessageClasses<I, M> {

    /**
     * message class
     */
    private Class<M> messageClass;
    /**
     * message value factory class
     */
    private Class<? extends MessageValueFactory<M>> messageValueFactoryClass;
    /**
     * message combiner class
     */
    private Class<? extends MessageCombiner<? super I, M>> messageCombinerClass;
    /**
     * whether message class was manually modified in this superstep
     */
    private boolean messageClassModified;
    /**
     * message encode and store type
     */
    private MessageEncodeAndStoreType messageEncodeAndStoreType;

    /**
     * Constructor
     */
    public DefaultMessageClasses() {}

    /**
     * Constructor
     *
     * @param messageClass              message class
     * @param messageValueFactoryClass  message value factory class
     * @param messageCombinerClass      message combiner class
     * @param messageEncodeAndStoreType message encode and store type
     */
    public DefaultMessageClasses(
            Class<M> messageClass,
            Class<? extends MessageValueFactory<M>> messageValueFactoryClass,
            Class<? extends MessageCombiner<? super I, M>> messageCombinerClass,
            MessageEncodeAndStoreType messageEncodeAndStoreType) {
        this.messageClass = messageClass;
        this.messageValueFactoryClass = messageValueFactoryClass;
        this.messageCombinerClass = messageCombinerClass;
        this.messageClassModified = false;
        this.messageEncodeAndStoreType = messageEncodeAndStoreType;
    }

    @Override
    public Class<M> getMessageClass() {
        return messageClass;
    }

    /**
     * Set message class
     *
     * @param messageClass message classs
     */
    public void setMessageClass(Class<M> messageClass) {
        this.messageClassModified = true;
        this.messageClass = messageClass;
    }

    @Override
    public MessageValueFactory<M> createMessageValueFactory(
            ImmutableClassesGiraphConfiguration conf) {
        if (messageValueFactoryClass.equals(DefaultMessageValueFactory.class)) {
            return new DefaultMessageValueFactory<>(messageClass, conf);
        }

        MessageValueFactory factory = ReflectionUtils.newInstance(messageValueFactoryClass, conf);
        if (!factory.newInstance().getClass().equals(messageClass)) {
            throw new IllegalStateException(
                    "Message factory "
                            + messageValueFactoryClass
                            + " creates "
                            + factory.newInstance().getClass().getName()
                            + ", but message type is "
                            + messageClass.getName());
        }
        return factory;
    }

    @Override
    public MessageCombiner<? super I, M> createMessageCombiner(
            ImmutableClassesGiraphConfiguration conf) {
        if (messageCombinerClass == null) {
            return null;
        }

        MessageCombiner combiner = ReflectionUtils.newInstance(messageCombinerClass, conf);
        if (combiner != null) {
            Preconditions.checkState(
                    combiner.createInitialMessage().getClass().equals(messageClass));
        }
        return combiner;
    }

    @Override
    public boolean useMessageCombiner() {
        return messageCombinerClass != null;
    }

    @Override
    public boolean ignoreExistingVertices() {
        return false;
    }

    @Override
    public MessageEncodeAndStoreType getMessageEncodeAndStoreType() {
        return messageEncodeAndStoreType;
    }

    public void setMessageEncodeAndStoreType(MessageEncodeAndStoreType messageEncodeAndStoreType) {
        this.messageEncodeAndStoreType = messageEncodeAndStoreType;
    }

    @Override
    public MessageClasses<I, M> createCopyForNewSuperstep() {
        return new DefaultMessageClasses<>(
                messageClass,
                messageValueFactoryClass,
                messageCombinerClass,
                messageEncodeAndStoreType);
    }

    @Override
    public void verifyConsistent(ImmutableClassesGiraphConfiguration conf) {
        Class<?>[] factoryTypes =
                ReflectionUtils.getTypeArguments(
                        MessageValueFactory.class, messageValueFactoryClass);
        ReflectionUtils.verifyTypes(
                messageClass, factoryTypes[0], "Message factory", messageValueFactoryClass);

        if (messageCombinerClass != null) {
            Class<?>[] combinerTypes =
                    ReflectionUtils.getTypeArguments(MessageCombiner.class, messageCombinerClass);
            ReflectionUtils.verifyTypes(
                    conf.getVertexIdClass(), combinerTypes[0], "Vertex id", messageCombinerClass);
            ReflectionUtils.verifyTypes(
                    messageClass, combinerTypes[1], "Outgoing message", messageCombinerClass);
        }
    }

    /**
     * Set message class if not set already in this superstep
     *
     * @param messageClass message class
     */
    public void setIfNotModifiedMessageClass(Class<M> messageClass) {
        if (!messageClassModified) {
            this.messageClass = messageClass;
        }
    }

    public void setMessageCombinerClass(
            Class<? extends MessageCombiner<? super I, M>> messageCombinerClass) {
        this.messageCombinerClass = messageCombinerClass;
    }

    public void setMessageValueFactoryClass(
            Class<? extends MessageValueFactory<M>> messageValueFactoryClass) {
        this.messageValueFactoryClass = messageValueFactoryClass;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeClass(messageClass, out);
        WritableUtils.writeClass(messageValueFactoryClass, out);
        WritableUtils.writeClass(messageCombinerClass, out);
        out.writeBoolean(messageClassModified);
        out.writeByte(messageEncodeAndStoreType.ordinal());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        messageClass = WritableUtils.readClass(in);
        messageValueFactoryClass = WritableUtils.readClass(in);
        messageCombinerClass = WritableUtils.readClass(in);
        messageClassModified = in.readBoolean();
        messageEncodeAndStoreType = messageEncodeAndStoreType.values()[in.readByte()];
    }
}
