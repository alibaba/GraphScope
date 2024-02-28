/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.wal;

import java.io.IOException;

/**
 * LogService provides persistent queues for realtime write.
 */
public interface LogService {

    void init();

    void destroy();

    boolean initialized();

    /**
     * Create a writer that can append data to a specific queue of LogService.
     * @return
     */
    LogWriter createWriter();

    /**
     * Create a reader can read data of specific queue from certain offset.
     * @param queueId
     * @param offset
     * @return
     */
    LogReader createReader(int queueId, long offset) throws IOException;

    /**
     * Create a reader that can read data of a specific queue from certain offset or timestamp
     * @param queueId
     * @param offset -1 if timestamp is specified.
     * @param timestamp -1 if offset is specified.
     * @return
     * @throws IOException
     */
    LogReader createReader(int queueId, long offset, long timestamp) throws IOException;

    /**
     * Delete all data before certain offset in the queue.
     *
     * @param queueId
     * @param offset
     */
    void deleteBeforeOffset(int queueId, long offset) throws IOException;
}
