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
 * Interface for handling Time related operations so that they can be mocked
 * for testing.
 */
public interface Time {
    /** Microseconds per millisecond */
    long US_PER_MS = 1000;
    /** Nanoseconds per microsecond */
    long NS_PER_US = 1000;
    /** Nanoseconds per millisecond */
    long NS_PER_MS = US_PER_MS * NS_PER_US;
    /** Milliseconds per second */
    long MS_PER_SECOND = 1000;
    /** Milliseconds per second (as float) */
    float MS_PER_SECOND_AS_FLOAT = MS_PER_SECOND * 1f;
    /** Microseconds per second */
    long US_PER_SECOND = US_PER_MS * MS_PER_SECOND;
    /** Microseconds per second (as float) */
    float US_PER_SECOND_AS_FLOAT = US_PER_SECOND * 1f;
    /** Nanoseconds per second */
    long NS_PER_SECOND = NS_PER_US * US_PER_SECOND;
    /** Nanoseconds per second (as float) */
    float NS_PER_SECOND_AS_FLOAT = NS_PER_SECOND * 1f;
    /** Seconds per hour */
    long SECONDS_PER_HOUR = 60 * 60;
    /** Seconds per day */
    long SECONDS_PER_DAY = 24 * SECONDS_PER_HOUR;
    /** Milliseconds per hour */
    long MS_PER_HOUR = SECONDS_PER_HOUR * MS_PER_SECOND;
    /** Milliseconds per day */
    long MS_PER_DAY = SECONDS_PER_DAY * MS_PER_SECOND;

    /**
     * Get the current milliseconds
     *
     * @return The difference, measured in milliseconds, between
     *         the current time and midnight, January 1, 1970 UTC.
     */
    long getMilliseconds();

    /**
     * Get the current microseconds
     *
     * @return The difference, measured in microseconds, between
     *         the current time and midnight, January 1, 1970 UTC.
     */
    long getMicroseconds();

    /**
     * Get the current nanoseconds
     *
     * @return The difference, measured in nanoseconds, between
     *         the current time and midnight, January 1, 1970 UTC.
     */
    long getNanoseconds();

    /**
     * Get the current seconds
     *
     * @return The difference, measured in seconds, between
     *         the current time and midnight, January 1, 1970 UTC.
     */
    int getSeconds();

    /**
     * Get the current date
     *
     * @return Current date
     */
    Date getCurrentDate();

    /**
     * Current thread should sleep for some number of milliseconds.
     *
     * @param milliseconds Milliseconds to sleep for
     * @throws InterruptedException
     */
    void sleep(long milliseconds) throws InterruptedException;
}
