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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;

import com.alibaba.maxgraph.logging.LogEvents.QueryEvent;
import com.alibaba.maxgraph.logging.LogEvents.QueryType;
import com.alibaba.maxgraph.logging.LogEvents.RuntimeEvent;
import com.alibaba.maxgraph.logging.LogEvents.ScheduleEvent;
import com.alibaba.maxgraph.logging.LogEvents.StoreEvent;
import com.alibaba.maxgraph.proto.RoleType;

import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

/**
 * @author xiafei.qiuxf
 * @date 2018-12-12
 */
public class LoggingTest {

    private Logger mockAlertLogger;
    private Logger mockSchedulerLogger;
    private Logger mockStoreLog;
    private Logger mockRealtimeLogger;
    private Logger mockQueryLogger;
    private Logger mockRuntimeLogger;

    private static void setFinalStatic(Field field, Object newValue) throws Exception {
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(null, newValue);
    }

    @BeforeMethod
    public void setUp() throws Exception {

        mockAlertLogger = Mockito.mock(org.slf4j.Logger.class);
        mockSchedulerLogger = Mockito.mock(org.slf4j.Logger.class);
        mockStoreLog = Mockito.mock(org.slf4j.Logger.class);
        mockRealtimeLogger = Mockito.mock(org.slf4j.Logger.class);
        mockQueryLogger = Mockito.mock(org.slf4j.Logger.class);
        mockRuntimeLogger = Mockito.mock(org.slf4j.Logger.class);

        setFinalStatic(Logging.class.getDeclaredField("LOG_ALERT"), mockAlertLogger);
        setFinalStatic(Logging.class.getDeclaredField("LOG_SCHEDULE"), mockSchedulerLogger);
        setFinalStatic(Logging.class.getDeclaredField("LOG_STORE"), mockStoreLog);
        setFinalStatic(Logging.class.getDeclaredField("LOG_REALTIME"), mockRealtimeLogger);
        setFinalStatic(Logging.class.getDeclaredField("LOG_QUERY"), mockQueryLogger);
        setFinalStatic(Logging.class.getDeclaredField("LOG_RUNTIME"), mockRuntimeLogger);

    }

    @AfterMethod
    public void tearDown() throws Exception {
        setFinalStatic(Logging.class.getDeclaredField("LOG_ALERT"), LoggerFactory.getLogger("AlertLog"));
        setFinalStatic(Logging.class.getDeclaredField("LOG_SCHEDULE"), LoggerFactory.getLogger("ScheduleLog"));
        setFinalStatic(Logging.class.getDeclaredField("LOG_STORE"), LoggerFactory.getLogger("SnapshotLog"));
        setFinalStatic(Logging.class.getDeclaredField("LOG_REALTIME"), LoggerFactory.getLogger("RealtimeLog"));
        setFinalStatic(Logging.class.getDeclaredField("LOG_QUERY"), LoggerFactory.getLogger("QueryLog"));
    }

    private static void assertLog(Logger logger, String... expectedFields) {
        ArgumentCaptor<String> line = ArgumentCaptor.forClass(String.class);
        verify(logger).info(line.capture());

        List<String> result = line.getAllValues();
        assertEquals(result.size(), 1);
        String[] fields = result.get(0).split("\u0001", -1);
        assertEquals(
            Arrays.asList(fields).subList(1, fields.length),
            Arrays.asList(expectedFields));
    }

    @Test
    public void testAlert() {
        Logging.alert("testGraph", RoleType.AM, 0, "alert log");
        assertLog(mockAlertLogger, "testGraph", "AM", "0", "alert log");
    }

    @Test
    public void testSchedule() {
        Logging.schedule("testGraph", ScheduleEvent.WORKER_HB_TIMEOUT, 9, "300 seconds");
        assertLog(mockSchedulerLogger, "testGraph", "AM", "0", "WORKER_HB_TIMEOUT", "9", "300 seconds");
    }

    @Test
    public void testQuery() {
        Logging.query("testGraph", RoleType.FRONTEND, 4, "999", QueryType.EXECUTE,
            QueryEvent.FRONT_RECEIVED, null, null, null, "g.V()");
        assertLog(mockQueryLogger, "testGraph", "FRONTEND", "4", "999", "EXECUTE",
            "FRONT_RECEIVED", "", "", "", "g.V()");
    }

    @Test
    public void testQuery2() {
        Logging.query("testGraph", RoleType.FRONTEND, 4, "999", QueryType.EXECUTE,
            QueryEvent.FRONT_RECEIVED, 7L, 8L, true, "g.V()");
        assertLog(mockQueryLogger, "testGraph", "FRONTEND", "4", "999", "EXECUTE",
            "FRONT_RECEIVED", "7", "8", "true", "g.V()");
    }

    @Test
    public void testStore() {
        Logging.store("testGraph", RoleType.COORDINATOR, 1, StoreEvent.SNAPSHOT_COOR_OFFLINE, 99, "see you");
        assertLog(mockStoreLog, "testGraph", "COORDINATOR", "1", "SNAPSHOT", "SNAPSHOT_COOR_OFFLINE", "99", "see you");
    }

    @Test
    public void testStore2() {
        Logging.store("testGraph", RoleType.COORDINATOR, 1, StoreEvent.SNAPSHOT_COOR_OFFLINE, null, "see you");
        assertLog(mockStoreLog, "testGraph", "COORDINATOR", "1", "SNAPSHOT", "SNAPSHOT_COOR_OFFLINE", "", "see you");
    }

    @Test
    public void testRealtime() {
        Logging.realtime("testGraph", RoleType.FRONTEND, 1, "client1", 100);
        assertLog(mockRealtimeLogger, "testGraph", "FRONTEND", "1", "client1", "100");
    }

    @Test
    public void testRuntime() {
        Logging.runtime("testGraph", RoleType.AM, 0, RuntimeEvent.GROUP_READY, 1, 1,10L, "xxx");
        assertLog(mockRuntimeLogger, "testGraph", "AM", "0", "GROUP_READY", "1", "1", "10", "xxx");
    }
}
