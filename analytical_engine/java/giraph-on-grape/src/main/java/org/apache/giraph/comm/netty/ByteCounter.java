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

package org.apache.giraph.comm.netty;


/** ByteCounter interface */
public interface ByteCounter {
    /**
     * Reset everything this object keeps track of
     */
    void resetAll();

    /**
     * @return A string containing all the metrics
     */
    String getMetrics();

    /**
     * Get the metrics if a given window of time has passed.  Return null
     * otherwise.  If the window is met, reset the metrics.
     *
     * @param minMsecsWindow Msecs of the minimum window
     * @return Metrics or else null if the window wasn't met
     */
    String getMetricsWindow(int minMsecsWindow);
}
