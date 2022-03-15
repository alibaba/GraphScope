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

package org.apache.giraph.io;

import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;
import java.util.List;

/**
 * Common interface for {@link VertexInputFormat} and {@link EdgeInputFormat}.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
public abstract class GiraphInputFormat<
                I extends WritableComparable, V extends Writable, E extends Writable>
        extends DefaultImmutableClassesGiraphConfigurable<I, V, E> {

    /**
     * Check that input is valid.
     *
     * @param conf Configuration
     */
    public abstract void checkInputSpecs(Configuration conf);

    /**
     * Get the list of input splits for the format.
     *
     * @param context           The job context
     * @param minSplitCountHint Minimum number of splits to create (hint)
     * @return The list of input splits
     * @throws IOException
     * @throws InterruptedException
     */
    public abstract List<InputSplit> getSplits(JobContext context, int minSplitCountHint)
            throws IOException, InterruptedException;

    //    /**
    //     * Write input split info to DataOutput.
    //     *
    //     * @param inputSplit InputSplit
    //     * @param dataOutput DataOutput
    //     */
    //    public void writeInputSplit(InputSplit inputSplit,
    //        DataOutput dataOutput) throws IOException {
    //        Text.writeString(dataOutput, inputSplit.getClass().getName());
    //        ((Writable) inputSplit).write(dataOutput);
    //    }
    //
    //    /**
    //     * Read input split info from DataInput.
    //     *
    //     * @param dataInput DataInput
    //     * @return InputSplit
    //     */
    //    public InputSplit readInputSplit(DataInput dataInput) throws IOException,
    //        ClassNotFoundException {
    //        String inputSplitClass = Text.readString(dataInput);
    //        InputSplit inputSplit = (InputSplit) ReflectionUtils.newInstance(
    //            getConf().getClassByName(inputSplitClass), getConf());
    //        ((Writable) inputSplit).readFields(dataInput);
    //        return inputSplit;
    //    }
}
