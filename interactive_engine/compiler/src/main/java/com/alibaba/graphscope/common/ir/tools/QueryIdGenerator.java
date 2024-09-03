/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.ir.tools;

import com.alibaba.graphscope.common.config.Configs;

import java.math.BigInteger;
import java.security.SecureRandom;

public class QueryIdGenerator {
    private final Configs configs;
    private final SecureRandom idGenerator;

    public QueryIdGenerator(Configs configs) {
        this.configs = configs;
        this.idGenerator = new SecureRandom();
    }

    /**
     * The function is to assign a unique job id for the query, which needs to ensure uniqueness in time and space.
     * Specifically, for queries a and b, the following two cases are required to not produce duplicate plan ids:
     *  1. When query a and b are sent to the same frontend but at different times;
     *  2. When query a and b are sent to different frontends, regardless if the timestamps are the same or not;
     *
     * This requires us to implement the functionality of UUID. The default Java {@link java.util.UUID} is composed of 128 bits,
     * with the first 64 bits determined by timestamp and the latter 64 bits composed of MAC address plus other items.
     * However, our job id is made up of 64 bits, and converting 128 bits into 64 bits might compromise the uniqueness of the id;
     *
     * Therefore, we adopt another method similar to random number generation. We use Java’s built-in {@link java.security.SecureRandom},
     * which can directly generate a random 64-bit id through {@link SecureRandom#nextLong()}. SecureRandom is a method of pseudo-random generation,
     * that is, deterministic generation of a random sequence with a random seed determined by a combination of timestamp + machine physical properties,
     * which basically meets our needs for plan id.
     *
     * @return Here, we use {@link BigInteger} to store the returned 64-bit UUID, which will be further converted to uint64 when being sent to the Pegasus engine.
     */
    public BigInteger generateId() {
        byte[] bytes = new byte[8];
        idGenerator.nextBytes(bytes);
        return new BigInteger(1, bytes);
    }

    public String generateName(BigInteger uniqueId) {
        return "ir_plan_" + uniqueId;
    }
}
