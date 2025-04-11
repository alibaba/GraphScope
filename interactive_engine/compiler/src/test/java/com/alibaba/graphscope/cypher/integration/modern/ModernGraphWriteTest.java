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

package com.alibaba.graphscope.cypher.integration.modern;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

public class ModernGraphWriteTest {
    private static Session session;

    @BeforeClass
    public static void beforeClass() {
        String neo4jServerUrl =
                System.getProperty("neo4j.bolt.server.url", "neo4j://localhost:7687");
        session = GraphDatabase.driver(neo4jServerUrl).session();
    }

    @Test
    public void insert_vertex_edge_test() {
        // create 2 new vertices and an edge between them
        session.run(
                        "CREATE (p1:person {name: 'test1', id: 2110, age: 32}) CREATE (p2:person"
                                + " {name: 'test2', id: 2111, age: 33}) CREATE (p1:person {id:"
                                + " 2110})-[:knows {weight: 0.1} ]->(p2:person {id: 2111})")
                .list();
        Result testP1 = session.run("MATCH (p:person {name: 'test1'}) RETURN p.name, p.id, p.age");
        Record record = testP1.list().get(0);
        Assert.assertEquals("test1", record.get(0).asString());
        Assert.assertEquals(2110, record.get(1).asInt());
        Assert.assertEquals(32, record.get(2).asInt());

        Result testP2 = session.run("MATCH (p:person {name: 'test2'}) RETURN p.name, p.id, p.age");
        record = testP2.list().get(0);
        Assert.assertEquals("test2", record.get(0).asString());
        Assert.assertEquals(2111, record.get(1).asInt());
        Assert.assertEquals(33, record.get(2).asInt());

        Result testEdge =
                session.run(
                        "MATCH (p1:person {id: 2110})-[f:knows]->(p2:person {id: 2111}) RETURN"
                                + " f");
        Assert.assertEquals(1, testEdge.list().size());
    }

    @AfterClass
    public static void afterClass() {
        if (session != null) {
            session.close();
        }
    }
}
