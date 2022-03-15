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

package org.apache.giraph.utils;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

/**
 * A Writable for ListArray containing instances of a class.
 *
 * @param <M> Message data
 */
public abstract class ArrayListWritable<M extends Writable> extends ArrayList<M>
        implements Writable, Configurable {

    /**
     * Defining a layout version for a serializable class.
     */
    private static final long serialVersionUID = 1L;
    /**
     * Used for instantiation
     */
    private Class<M> refClass = null;

    /**
     * Configuration
     */
    private Configuration conf;

    /**
     * Using the default constructor requires that the user implement setClass(), guaranteed to be
     * invoked prior to instantiation in readFields()
     */
    public ArrayListWritable() {}

    /**
     * Constructor with another {@link ArrayListWritable}.
     *
     * @param arrayListWritable Array list to be used internally.
     */
    public ArrayListWritable(ArrayListWritable<M> arrayListWritable) {
        super(arrayListWritable);
    }

    /**
     * This constructor allows setting the refClass during construction.
     *
     * @param refClass internal type class
     */
    public ArrayListWritable(Class<M> refClass) {
        super();
        this.refClass = refClass;
    }

    /**
     * This is a one-time operation to set the class type
     *
     * @param refClass internal type class
     */
    public void setClass(Class<M> refClass) {
        if (this.refClass != null) {
            throw new RuntimeException(
                    "setClass: refClass is already set to " + this.refClass.getName());
        }
        this.refClass = refClass;
    }

    /**
     * Subclasses must set the class type appropriately and can use setClass(Class&lt;M&gt;
     * refClass) to do it.
     */
    public abstract void setClass();

    @Override
    public void readFields(DataInput in) throws IOException {
        if (this.refClass == null) {
            setClass();
        }

        clear(); // clear list before storing values
        int numValues = in.readInt(); // read number of values
        ensureCapacity(numValues);
        for (int i = 0; i < numValues; i++) {
            M value = ReflectionUtils.newInstance(refClass, conf);
            value.readFields(in); // read a value
            add(value); // store it in values
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        int numValues = size();
        out.writeInt(numValues); // write number of values
        for (int i = 0; i < numValues; i++) {
            get(i).write(out);
        }
    }

    @Override
    public final Configuration getConf() {
        return conf;
    }

    @Override
    public final void setConf(Configuration conf) {
        this.conf = conf;
    }
}
