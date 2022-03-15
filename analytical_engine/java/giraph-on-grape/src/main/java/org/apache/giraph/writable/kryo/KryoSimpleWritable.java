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
package org.apache.giraph.writable.kryo;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import org.apache.giraph.writable.kryo.markers.KryoIgnoreWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Class which you can extend to get all serialization/deserialization done automagically.
 * <p>
 * Usage of this class is similar to KryoWritable but unlike KryoWritable, this class does not
 * support recursive/nested objects to provide better performance.
 * <p>
 * If the underlying stream is a kryo output stream than the read/write happens with a kryo object
 * that doesn't track references, providing significantly better performance.
 */
public abstract class KryoSimpleWritable implements KryoIgnoreWritable {

    @Override
    public final void write(DataOutput out) throws IOException {
        if (out instanceof Output) {
            Output outp = (Output) out;
            HadoopKryo.writeWithKryoOutOfObject(HadoopKryo.getNontrackingKryo(), outp, this);
        } else {
            HadoopKryo.writeOutOfObject(out, this);
        }
    }

    @Override
    public final void readFields(DataInput in) throws IOException {
        if (in instanceof Input) {
            Input inp = (Input) in;
            HadoopKryo.readWithKryoIntoObject(HadoopKryo.getNontrackingKryo(), inp, this);
        } else {
            HadoopKryo.readIntoObject(in, this);
        }
    }
}
