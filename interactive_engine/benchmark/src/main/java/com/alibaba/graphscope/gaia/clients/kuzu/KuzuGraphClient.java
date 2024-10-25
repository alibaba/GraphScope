/**
 * Copyright 2024 Alibaba Group Holding Limited.
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
package com.alibaba.graphscope.gaia.clients.kuzu;

import com.alibaba.graphscope.gaia.clients.GraphClient;
import com.alibaba.graphscope.gaia.clients.GraphResultSet;
import com.kuzudb.KuzuConnection;
import com.kuzudb.KuzuDatabase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KuzuGraphClient implements GraphClient {
    KuzuConnection conn;
    private static Logger logger = LoggerFactory.getLogger(KuzuGraphClient.class);

    public KuzuGraphClient(String dbPath) {
        KuzuDatabase db = new KuzuDatabase(dbPath);
        try {
            this.conn = new KuzuConnection(db);
            logger.info("Connected to kuzu server at " + dbPath);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to create connection with kuzu server");
        }
    }

    public void setMaxNumThreadForExec(long parallelism) {
        try {
            conn.setMaxNumThreadForExec(parallelism);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public GraphResultSet submit(String query) {
        try {
            return new KuzuGraphResult(conn.query(query));
        } catch (Exception e) {
            e.printStackTrace();
            return new KuzuGraphResult();
        }
    }

    @Override
    public void close() {
        try {
            conn.destroy();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
