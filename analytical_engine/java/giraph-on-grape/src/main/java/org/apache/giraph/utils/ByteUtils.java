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

/**
 * Utilities class for byte operations and constants
 */
public class ByteUtils {

    /**
     * Bytes used in a boolean
     */
    public static final int SIZE_OF_BOOLEAN = 1;
    /**
     * Bytes used in a byte
     */
    public static final int SIZE_OF_BYTE = 1;
    /**
     * Bytes used in a char
     */
    public static final int SIZE_OF_CHAR = Character.SIZE / Byte.SIZE;
    /**
     * Bytes used in a short
     */
    public static final int SIZE_OF_SHORT = Short.SIZE / Byte.SIZE;
    /**
     * Bytes used in an int
     */
    public static final int SIZE_OF_INT = Integer.SIZE / Byte.SIZE;
    /**
     * Bytes used in a long
     */
    public static final int SIZE_OF_LONG = Long.SIZE / Byte.SIZE;
    /**
     * Bytes used in a float
     */
    public static final int SIZE_OF_FLOAT = Float.SIZE / Byte.SIZE;
    /**
     * Bytes used in a double
     */
    public static final int SIZE_OF_DOUBLE = Double.SIZE / Byte.SIZE;

    /**
     * Private Constructor
     */
    private ByteUtils() {}
}
