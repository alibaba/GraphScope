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
package org.apache.giraph.types.ops.collections.array;

import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.booleans.BooleanArrays;
import it.unimi.dsi.fastutil.booleans.BooleanCollection;
import it.unimi.dsi.fastutil.booleans.BooleanList;

import org.apache.giraph.function.Consumer;
import org.apache.giraph.function.Predicate;
import org.apache.giraph.function.primitive.BooleanConsumer;
import org.apache.giraph.function.primitive.BooleanPredicate;
import org.apache.giraph.types.ops.BooleanTypeOps;
import org.apache.giraph.types.ops.PrimitiveTypeOps;
import org.apache.giraph.types.ops.collections.ResettableIterator;
import org.apache.giraph.types.ops.collections.WBooleanCollection;
import org.apache.giraph.utils.Varint;
import org.apache.hadoop.io.BooleanWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

// AUTO-GENERATED class via class:
// org.apache.giraph.generate.GeneratePrimitiveClasses

/**
 * Writable extension of BooleanArrayList, as well as BooleanWritable implementation of WArrayList.
 */
public class WBooleanArrayList extends BooleanArrayList
        implements WArrayList<BooleanWritable>, WBooleanCollection {

    /**
     * Creates a new array list with {@link #DEFAULT_INITIAL_CAPACITY} capacity.
     */
    public WBooleanArrayList() {
        super();
    }

    /**
     * Creates a new array list with given capacity.
     *
     * @param capacity the initial capacity of the array list (may be 0).
     */
    public WBooleanArrayList(int capacity) {
        super(capacity);
    }

    /**
     * Creates a new array list and fills it with a given type-specific collection.
     *
     * @param c a type-specific collection that will be used to fill the array list.
     */
    public WBooleanArrayList(BooleanCollection c) {
        super(c);
    }

    /**
     * Creates a new array list and fills it with a given type-specific list.
     *
     * @param l a type-specific list that will be used to fill the array list.
     */
    public WBooleanArrayList(BooleanList l) {
        super(l);
    }

    /**
     * Write a potentially null list to a DataOutput. Null list is written equivalently as an empty
     * list. Use WritableUtils.writeIfNotNullAndObject if distinction is needed.
     *
     * @param list Array list to be written
     * @param out  Data output
     */
    public static void writeOrNull(WBooleanArrayList list, DataOutput out) throws IOException {
        if (list == null) {
            Varint.writeUnsignedVarInt(0, out);
        } else {
            list.write(out);
        }
    }

    /**
     * Read array list from the DataInput stream into a newly created object.
     *
     * @param in Data input
     * @return New read array list object.
     */
    public static WBooleanArrayList readNew(DataInput in) throws IOException {
        int size = Varint.readSignedVarInt(in);
        WBooleanArrayList list = new WBooleanArrayList(size);
        list.readElements(in, size);
        return list;
    }

    @Override
    public PrimitiveTypeOps<BooleanWritable> getElementTypeOps() {
        return BooleanTypeOps.INSTANCE;
    }

    @Override
    public int capacity() {
        return elements().length;
    }

    @Override
    public void setCapacity(int n) {
        if (n >= capacity()) {
            ensureCapacity(n);
        } else {
            trim(n);
        }
    }

    @Override
    public void addW(BooleanWritable value) {
        add(value.get());
    }

    @Override
    public void getIntoW(int index, BooleanWritable to) {
        to.set(getBoolean(index));
    }

    @Override
    public void popIntoW(BooleanWritable to) {
        to.set(popBoolean());
    }

    @Override
    public void setW(int index, BooleanWritable value) {
        set(index, value.get());
    }

    @Override
    public void fillW(int from, int to, BooleanWritable value) {
        if (to > size()) {
            throw new ArrayIndexOutOfBoundsException(
                    "End index (" + to + ") is greater than array length (" + size() + ")");
        }
        Arrays.fill(elements(), from, to, value.get());
    }

    @Override
    public ResettableIterator<BooleanWritable> fastIteratorW() {
        return fastIteratorW(getElementTypeOps().create());
    }

    @Override
    public ResettableIterator<BooleanWritable> fastIteratorW(BooleanWritable iterationValue) {
        return WArrayListPrivateUtils.fastIterator(this, iterationValue);
    }

    @Override
    public void fastForEachW(Consumer<BooleanWritable> f) {
        WArrayListPrivateUtils.fastForEach(this, f, getElementTypeOps().create());
    }

    @Override
    public boolean fastForEachWhileW(Predicate<BooleanWritable> f) {
        return WArrayListPrivateUtils.fastForEachWhile(this, f, getElementTypeOps().create());
    }

    /**
     * Traverse all elements of the array list, calling given function on each element, or until
     * predicate returns false.
     *
     * @param f Function to call on each element.
     */
    public void forEachBoolean(BooleanConsumer f) {
        for (int i = 0; i < size(); ++i) {
            f.apply(getBoolean(i));
        }
    }

    /**
     * Traverse all elements of the array list, calling given function on each element.
     *
     * @param f Function to call on each element.
     * @return true if the predicate returned true for all elements, false if it returned false for
     * some element.
     */
    public boolean forEachWhileBoolean(BooleanPredicate f) {
        for (int i = 0; i < size(); ++i) {
            if (!f.apply(getBoolean(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void sort() {
        BooleanArrays.quickSort(elements(), 0, size());
    }

    @Override
    public void writeElements(DataOutput out) throws IOException {
        for (int i = 0; i < size; i++) {
            out.writeBoolean(a[i]);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Varint.writeUnsignedVarInt(size, out);
        writeElements(out);
    }

    @Override
    public void readElements(DataInput in, int size) throws IOException {
        this.size = size;
        resizeArrayForRead(size);
        for (int i = 0; i < size; i++) {
            a[i] = in.readBoolean();
        }
    }

    /**
     * Resize array for reading given number of elements.
     *
     * @param size Number of elements that will be read.
     */
    protected void resizeArrayForRead(int size) {
        if (size != a.length) {
            a = new boolean[size];
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        readElements(in, Varint.readUnsignedVarInt(in));
    }

    /**
     * Variant of WBooleanArrayList that doesn't reallocate smaller backing array on consecutive
     * readFields/readElements calls, and so is suitable for reusable use. (and backing array will
     * only grow on readFields/readElements calls)
     */
    public static class WReusableBooleanArrayList extends WBooleanArrayList {

        /**
         * Constructor
         */
        public WReusableBooleanArrayList() {
            super();
        }

        /**
         * Constructor
         *
         * @param capacity Capacity
         */
        public WReusableBooleanArrayList(int capacity) {
            super(capacity);
        }

        /**
         * Read array list from DataInput stream, into a given list if not null, or creating a new
         * list if given list is null.
         *
         * @param list Array list to be written
         * @param in   Data input
         * @return Passed array list, or a new one if null is passed
         */
        public static WReusableBooleanArrayList readIntoOrCreate(
                WReusableBooleanArrayList list, DataInput in) throws IOException {
            int size = Varint.readUnsignedVarInt(in);
            if (list == null) {
                list = new WReusableBooleanArrayList(size);
            }
            list.readElements(in, size);
            return list;
        }

        @Override
        protected void resizeArrayForRead(int size) {
            if (size > a.length) {
                a = new boolean[size];
            }
        }
    }
}
