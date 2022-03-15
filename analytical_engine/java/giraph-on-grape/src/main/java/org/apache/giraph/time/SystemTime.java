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

package org.apache.giraph.time;

import java.util.Date;

/**
 * Implementation of Time that is thread-safe and should be used in production.
 */
public class SystemTime implements Time {

    /**
     * Single instance of this object
     */
    private static final SystemTime SINGLE_TIME = new SystemTime();

    /**
     * Get an instance (shared) of this object
     *
     * @return Instance of this object
     */
    public static Time get() {
        return SINGLE_TIME;
    }

    @Override
    public long getMilliseconds() {
        return System.currentTimeMillis();
    }

    @Override
    public long getMicroseconds() {
        return getNanoseconds() / NS_PER_US;
    }

    @Override
    public long getNanoseconds() {
        return System.nanoTime();
    }

    @Override
    public int getSeconds() {
        return (int) (getMilliseconds() / MS_PER_SECOND);
    }

    @Override
    public Date getCurrentDate() {
        return new Date();
    }

    @Override
    public void sleep(long milliseconds) throws InterruptedException {
        Thread.sleep(milliseconds);
    }
}
