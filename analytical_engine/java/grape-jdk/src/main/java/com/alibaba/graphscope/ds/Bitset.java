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

package com.alibaba.graphscope.ds;

import static com.alibaba.graphscope.utils.CppClassName.GRAPE_BIT_SET;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_BIT_SET_H;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_WORKER_COMM_SPEC_H;
import static com.alibaba.graphscope.utils.JNILibraryName.JNI_LIBRARY_NAME;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;

@FFIGen(library = JNI_LIBRARY_NAME)
@CXXHead(value = {GRAPE_WORKER_COMM_SPEC_H, GRAPE_BIT_SET_H})
@FFITypeAlias(GRAPE_BIT_SET)
/**
 * Java wrapper for <a
 * href="https://github.com/alibaba/libgrape-lite/blob/master/grape/utils/bitset.h">grape
 * BitSet</a>, an efficient implementation for bitset.
 */
public interface Bitset extends FFIPointer, CXXPointer {

    Factory factory = FFITypeFactory.getFactory(Factory.class, Bitset.class);

    /**
     * Init the bitset with a initial size.
     *
     * @param size initial size.
     */
    void init(long size);

    /** Clear this bitset. */
    void clear();

    /**
     * Check empty.
     *
     * @return true if empty.
     */
    boolean empty();

    // void parallel_clear(int thread_num);

    /**
     * Check empty or not in parallel.
     *
     * @param begin starting index.
     * @param end ending index.
     * @return true if empty.
     */
    @FFINameAlias("partial_empty")
    boolean partialEmpty(long begin, long end);

    /**
     * Get the flag at index i.
     *
     * @param i index
     * @return true if index i has been set to true.
     */
    @FFINameAlias("get_bit")
    boolean getBit(long i);

    /**
     * Set the flag for index i to true
     *
     * @param i index
     */
    @FFINameAlias("set_bit")
    void setBit(long i);

    /**
     * Set the bit at index i with returned result.
     *
     * @param i index
     * @return result
     */
    @FFINameAlias("set_bit_with_ret")
    boolean setBitWithRet(long i);

    /**
     * Reset bit at index i.
     *
     * @param i index.
     */
    @FFINameAlias("reset_bit")
    void resetBit(long i);

    /**
     * Reset with return value.
     *
     * @param i index
     * @return return value.
     */
    @FFINameAlias("reset_bit_with_ret")
    boolean resetBitWithRet(long i);

    /**
     * Swap the underlying storage with another BitSet
     *
     * @param other the other bitset.
     */
    void swap(@CXXReference Bitset other);

    /**
     * The number of bits set to true.
     *
     * @return the number of trues.
     */
    long count();

    /**
     * The number of bits set to true, in a specified range
     *
     * @param begin begin index.
     * @param end end index.
     * @return the count value.
     */
    long partial_count(long begin, long end);

    /**
     * Get the underlying representation word at index i.
     *
     * @param i index
     * @return 64-bit long
     */
    long get_word(long i);

    /** Factory for BitSet. */
    @FFIFactory
    interface Factory {
        /**
         * Create a new instance for BitSet.Remember to call {@link Bitset#init(long)} to init the
         * obj. The actual storage is in C++ memory.
         *
         * @return a new instance.
         */
        Bitset create();
    }
}
