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
package org.apache.giraph.factories;

import com.google.common.base.MoreObjects;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;

/**
 * Factory class to create default message values.
 *
 * @param <M> Message Value
 */
public class DefaultMessageValueFactory<M extends Writable>
    implements MessageValueFactory<M> {
    /** Message value class */
    private final Class<M> messageValueClass;
    /** Configuration */
    private final ImmutableClassesGiraphConfiguration conf;

    /**
     * Constructor
     * @param messageValueClass message value class
     * @param conf configuration
     */
    public DefaultMessageValueFactory(Class<M> messageValueClass,
        ImmutableClassesGiraphConfiguration conf) {
        this.messageValueClass = messageValueClass;
        this.conf = conf;
    }

    @Override public M newInstance() {
        return WritableUtils.createWritable(messageValueClass, conf);
    }

    @Override public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("messageValueClass", messageValueClass)
            .toString();
    }
}
