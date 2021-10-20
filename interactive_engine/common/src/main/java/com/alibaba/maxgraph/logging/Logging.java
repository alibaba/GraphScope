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
package com.alibaba.maxgraph.logging;

import com.alibaba.maxgraph.logging.LogEvents.QueryEvent;
import com.alibaba.maxgraph.logging.LogEvents.ScheduleEvent;
import com.alibaba.maxgraph.proto.RoleType;

import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author xiafei.qiuxf
 * @date 2018-12-12
 */
public class Logging {
    private static final String FIELD_SEP = "\u0001";

    private static final Logger LOG_ALERT = LoggerFactory.getLogger("AlertLog");
    private static final Logger LOG_SCHEDULE = LoggerFactory.getLogger("ScheduleLog");
    private static final Logger LOG_STORE = LoggerFactory.getLogger("StoreLog");
    private static final Logger LOG_REALTIME = LoggerFactory.getLogger("RealtimeLog");
    private static final Logger LOG_QUERY = LoggerFactory.getLogger("QueryLog");
    private static final Logger LOG_RUNTIME = LoggerFactory.getLogger("RuntimeLog");

    private static String generateLogRecord(String graphName, RoleType role, int serverId, Object... fields) {
        long time = System.currentTimeMillis();
        StringBuilder sb = new StringBuilder();
        sb.append(time).append(FIELD_SEP)
            .append(graphName).append(FIELD_SEP)
            .append(role).append(FIELD_SEP)
            .append(serverId).append(FIELD_SEP);
        Joiner.on(FIELD_SEP).appendTo(sb, fields);
        return sb.toString();
    }

    /**
     * Write alert log.
     *
     * @param graphName graph name.
     * @param role      role type of the server who's writing this log line.
     * @param serverId  server id of the server who's writing this log line.
     * @param msg       alert message.
     */
    public static void alert(String graphName, RoleType role, int serverId, String msg) {
        LOG_ALERT.info(generateLogRecord(graphName, role, serverId, msg));
    }

    /**
     * Write schedule log.
     *
     * @param graphName      graph name.
     * @param event          schedule event.
     * @param targetServerId the server AM is operating on.
     * @param msg            extra infomation.
     */
    public static void schedule(String graphName, ScheduleEvent event, int targetServerId, String msg) {
        LOG_SCHEDULE.info(generateLogRecord(graphName, RoleType.AM, 0, event, targetServerId, msg));
    }

    /**
     * Write query log.
     *
     * @param graphName graph name
     * @param role      role type of the server who's writing this log line.
     * @param serverId  server id of the server who's writing this log line.
     * @param queryId   query id, a 64 bit integer.
     * @param queryType EXECUTE / PREPARE / Query,
     * @param event     query event
     * @param latency   latency since query arrived on this server (front / executor) optional
     * @param resultNum number of result, optional
     * @param success   if query successes, optional
     */
    public static void query(String graphName, RoleType role, int serverId,
                             String queryId, LogEvents.QueryType queryType, QueryEvent event,
                             Long latency, Long resultNum, Boolean success, String query) {
        LOG_QUERY.info(generateLogRecord(graphName, role, serverId, queryId, queryType, event,
            latency == null ? "" : latency,
            resultNum == null ? "" : resultNum,
            success == null ? "" : success,
            query == null ? "": query));
    }

    /**
     * Write store log.
     *
     * @param graphName graph name
     * @param role      role type of the server who's writing this log line.
     * @param serverId  server id of the server who's writing this log line.
     * @param event     store event.
     * @param key       event's key, snapshot id for snapshot events, type name for DDL, et.
     * @param message   extra message.
     */
    public static void store(String graphName, RoleType role, int serverId,
                             LogEvents.StoreEvent event, Object key, String message) {
        LOG_STORE.info(generateLogRecord(graphName, role, serverId, event.type, event,
            key == null ? "" : key,
            message));
    }

    public static void realtime(String graphName, RoleType role, int serverId,
                                String clientSession, int recordNum) {
        LOG_REALTIME.info(generateLogRecord(graphName, role, serverId,
            clientSession == null ? "" : clientSession,
            recordNum));
    }

    /**
     * Write runtime log
     *
     * @param graphName graph name
     * @param role      role type of the server who's writing this log line.
     * @param serverId  server id of the server who's writing this log line.
     * @param groupId  partition group id.
     * @param workerId  worker id in a partiton group.
     * @param version   optional.
     * @param message   extra message.
     */
    public static void runtime(String graphName, RoleType role, int serverId,
                               LogEvents.RuntimeEvent event, int groupId, int workerId, Long version,
                               String message) {
        LOG_RUNTIME.info(generateLogRecord(graphName, role, serverId, event, groupId, workerId,
            version == null ? "" : version,
            message));
    }
}
