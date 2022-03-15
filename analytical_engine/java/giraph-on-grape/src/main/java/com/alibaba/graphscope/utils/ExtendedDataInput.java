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

import java.io.DataInput;

/**
 * Add some functionality to data input
 */
public interface ExtendedDataInput extends DataInput {

    /**
     * Get the position of what has been read
     *
     * @return How many bytes have been read?
     */
    int getPos();

    /**
     * How many bytes are available?
     *
     * @return Bytes available
     */
    int available();

    /**
     * Check if we read everything from the input
     *
     * @return True iff we read everything from the input
     */
    boolean endOfInput();
}
