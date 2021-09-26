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

package org.apache.giraph.bsp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * This InputSplit will not give any ordering or location data.
 * It is used internally by BspInputFormat (which determines
 * how many tasks to run the application on).  Users should not use this
 * directly.
 */
public class BspInputSplit extends InputSplit implements Writable {
    /** Number of splits */
    private int numSplits = -1;
    /** Split index */
    private int splitIndex = -1;

    /**
     * Reflection constructor.
     */
    public BspInputSplit() { }

    /**
     * Constructor used by {@link BspInputFormat}.
     *
     * @param splitIndex Index of this split.
     * @param numSplits Total number of splits.
     */
    public BspInputSplit(int splitIndex, int numSplits) {
        this.splitIndex = splitIndex;
        this.numSplits = numSplits;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[]{};
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        splitIndex = in.readInt();
        numSplits = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(splitIndex);
        out.writeInt(numSplits);
    }

    /**
     * Get the index of this split.
     *
     * @return Index of this split.
     */
    public int getSplitIndex() {
        return splitIndex;
    }

    /**
     * Get the number of splits for this application.
     *
     * @return Total number of splits.
     */
    public int getNumSplits() {
        return numSplits;
    }

    @Override
    public String toString() {
        return "'" + getClass().getCanonicalName() +
            ", index=" + getSplitIndex() + ", num=" + getNumSplits();
    }
}
