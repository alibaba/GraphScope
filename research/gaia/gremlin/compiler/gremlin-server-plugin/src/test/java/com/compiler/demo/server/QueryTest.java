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
package com.compiler.demo.server;

import com.alibaba.pegasus.builder.AbstractBuilder;
import com.compiler.demo.server.idmaker.IdMaker;
import com.compiler.demo.server.idmaker.IncrementalQueryIdMaker;
import com.compiler.demo.server.idmaker.TagIdMaker;
import com.compiler.demo.server.plan.PlanUtils;
import com.compiler.demo.server.plan.translator.TraversalTranslator;
import com.compiler.demo.server.plan.translator.builder.PlanConfig;
import com.compiler.demo.server.plan.translator.builder.TraversalBuilder;
import com.compiler.demo.server.processor.MaxGraphOpProcessor;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class QueryTest {
    private static Logger logger = LoggerFactory.getLogger(QueryTest.class);
    private static final GraphTraversalSource g = EmptyGraph.instance().traversal();

    public static void main(String[] args) throws Exception {
        // read default engine conf
        GlobalEngineConf.setDefaultSysConf(JsonUtils.fromJson(
                FileUtils.readFileToString(new File("conf/system.args.json"), StandardCharsets.UTF_8), new TypeReference<Map<String, Object>>() {
                })
        );
        IdMaker queryIdMaker = new IncrementalQueryIdMaker();
        Traversal testTraversal = getTestTraversal();
        long queryId = (long) queryIdMaker.getId(testTraversal.asAdmin());
        AbstractBuilder job = new TraversalTranslator((new TraversalBuilder(testTraversal.asAdmin()))
                .addConfig(PlanConfig.QUERY_ID, queryId)
                .addConfig(PlanConfig.TAG_ID_MAKER, new TagIdMaker(testTraversal.asAdmin()))
                .addConfig(PlanConfig.QUERY_CONFIG, PlanUtils.getDefaultConfig(queryId))).translate();
        PlanUtils.print(job);
    }

    public static Traversal getTestTraversal() {
        Traversal traversal = CR_1_1();
        MaxGraphOpProcessor.applyStrategy(traversal);
        return traversal;
    }

    public static Traversal CR_1_1() {
        return g.V().hasLabel("PERSON").has("id", 30786325583618L)
                .out()
                .both("PERSON_KNOWS_PERSON")
                .union(__.identity(),
                        __.both("PERSON_KNOWS_PERSON").union(__.identity(), __.both("PERSON_KNOWS_PERSON")))
                .has("id", P.neq(30786325583618L)).has("firstName", P.eq("Chau")).as("a")
                .path().count(Scope.local)
                .as("b")
                .select("a")
                .order().by(__.select("b"), Order.desc).by("lastName").by("id")
                .limit(20)
                .select("a", "b");
    }

    public static Traversal CR_1_2() {
        return g.V(1).union(
                __.out("COMMENT_HASCREATOR_PERSON").values("name"),
                __.outE("COMMENT_HASCREATOR_PERSON").as("study").inV().as("u").out("COMMENT_HASCREATOR_PERSON").as("city")
                        .select("study", "u", "city"),
                __.outE("COMMENT_HASCREATOR_PERSON").as("we").inV().as("company").out("COMMENT_HASCREATOR_PERSON").as("country")
                        .select("we", "company", "country"));
    }

    public static Traversal CR_2() {
        return g.V().hasLabel("PERSON").has("id", 17592186052613L)
                .both("COMMENT_HASCREATOR_PERSON").as("p")
                .in("COMMENT_HASCREATOR_PERSON").has("creationDate", P.lte(20121128000000000L))
                .order().by("creationDate", Order.desc).by("id", Order.asc)
                .limit(20).as("m")
                .select("p", "m")
                .by(__.valueMap("id", "firstName", "lastName"))
                .by(__.valueMap("id", "imageFile", "creationDate", "content"));
    }

    public static Traversal CR_3_1() {
        return g.V().hasLabel("PERSON").has("id", 17592186055119L)
                .both("PERSON_KNOWS_PERSON")
                .union(__.identity(), __.both("PERSON_KNOWS_PERSON"))
                .filter(__.out("COMMENT_HASCREATOR_PERSON")
                        .has("name", P.without("United_Kingdom", "United_States")))
                .filter(__.in("COMMENT_HASCREATOR_PERSON")
                        .has("creationDate", P.inside(20110601000000000L, 20111013000000000L))
                        .out("COMMENT_HASCREATOR_PERSON").has("name", P.eq("United_Kingdom").or(P.eq("United_States"))))
                .group()
                .by()
                .by(__.in("COMMENT_HASCREATOR_PERSON")
                        .has("creationDate", P.inside(20110601000000000L, 20111013000000000L))
                        .out("COMMENT_HASCREATOR_PERSON")
                        .has("name", "United_Kingdom").count())
                .unfold()
                .order().by(__.select(Column.values), Order.desc).by(__.select(Column.keys).values("id"))
                .limit(20);
    }

    public static Traversal CR_3_2() {
        return g.V(1)
                .in("PERSON_KNOWS_PERSON")
                .has("creationDate", P.inside(20110601000000000L, 20110713000000000L))
                .filter(__.out("PERSON_KNOWS_PERSON").has("name", P.eq("United_States")))
                .count();
    }

    public static Traversal CR_5() {
        return g.V().hasLabel("PERSON").has("id", 21990232560302L)
                .both("PERSON_KNOWS_PERSON")
                .union(__.identity(), __.both("PERSON_KNOWS_PERSON"))
//                .as("p")
                .inE("PERSON_KNOWS_PERSON").has("joinDate", P.gt(20120901000000000L))
                .outV()
                .group()
                .by()
                .by(__.out("PERSON_KNOWS_PERSON").hasLabel("POST").out("PERSON_KNOWS_PERSON"))
//                .unfold();
                .order()
                .by(__.select(Column.values), Order.desc)
                .by(__.select(Column.keys).values("id"), Order.asc)
                .limit(20);
    }

    public static Traversal CR_6() {
        return g.V().hasLabel("PERSON").has("id", 30786325583618L)
                .both("PERSON_KNOWS_PERSON").union(__.identity(), __.both("PERSON_KNOWS_PERSON"))
                .has("id", P.neq(30786325583618L))
                .in("PERSON_KNOWS_PERSON").hasLabel("POST")
                .filter(__.out("PERSON_KNOWS_PERSON").has("name", P.eq("Angola")))
                .out("PERSON_KNOWS_PERSON")
                .has("name", P.neq("PERSON_KNOWS_PERSON"))
                .groupCount()
                .unfold()
                .order()
                .by(__.select(Column.values), Order.desc)
                .by(__.select(Column.keys).values("name"), Order.asc)
                .limit(10);
    }

    public static Traversal CR_7() {
        return g.V().hasLabel("PERSON").has("id", 17592186053137L)
                .in("PERSON_KNOWS_PERSON").as("message")
                .inE("PERSON_KNOWS_PERSON").as("like")
                .values("creationDate")
                .as("likedate")
                .order().by(Order.asc).limit(10)
                .select("like")
                .outV().as("liker")
                .order()
                .by(__.select("likedate"), Order.desc)
                .by("id", Order.asc).limit(20)
                .select("message", "likedate", "liker")
                .by(__.valueMap("id", "content", "imageFile"))
                .by()
                .by(__.valueMap("id", "firstName", "lastName"));
    }

    public static Traversal CR_8() {
        return g.V().hasLabel("PERSON").has("id", 24189255818757L)
                .in("PERSON_KNOWS_PERSON")
                .in("PERSON_KNOWS_PERSON")
                .hasLabel("COMMENT").as("comment")
                .order().by("creationDate", Order.desc).by("id", Order.asc)
                .limit(20)
                .out("PERSON_KNOWS_PERSON").as("commenter")
                .select("commenter", "comment")
                .by(__.valueMap("id", "firstName", "lastName"))
                .by(__.valueMap("creationDate", "id", "content"));
    }

    public static Traversal CR_9() {
        return g.V().hasLabel("PERSON").has("id", 13194139542834L)
                .both("PERSON_KNOWS_PERSON")
                .union(__.identity(), __.both("PERSON_KNOWS_PERSON"))
                .has("id", P.neq(13194139542834L)).as("friends")
                .in("PERSON_KNOWS_PERSON").has("creationDate", P.lt(20111217000000000L)).as("post")
                .order().by("creationDate", Order.desc).by("id", Order.asc).limit(20)
                .select("friends", "post")
                .by(__.valueMap("id", "firstName", "lastName"))
                .by(__.valueMap("id", "content", "imageFile", "creationDate"));
    }

    public static Traversal CR_11() {
        return g.V().hasLabel("PERSON").has("id", 30786325583618L).as("root")
                .both("PERSON_KNOWS_PERSON").union(__.identity(), __.both("PERSON_KNOWS_PERSON")).dedup()
                .has("id", P.neq(30786325583618L)).as("friends")
                .outE("PERSON_KNOWS_PERSON").has("workFrom", P.lt(2010)).as("startWork")
                .values("workFrom").as("works").select("startWork")
                .inV().as("comp")
                .values("name").as("orgname").select("comp")
                .out("PERSON_KNOWS_PERSON").has("name", "Laos")
                .select("friends")
                .order()
                .by(__.select("works"), Order.desc)
                .by("id", Order.asc)
                .by(__.select("orgname"), Order.desc)
                .limit(10).select("friends", "orgname", "works")
                .by(__.valueMap("id", "firstName", "lastName"))
                .by()
                .by();
    }

    public static Traversal CR_12() {
        return g.V().hasLabel("PERSON").has("id", 17592186052613L)
                .both("PERSON_KNOWS_PERSON").as("friend").in("PERSON_KNOWS_PERSON").hasLabel("PERSON")
                .filter(__.out("PERSON_KNOWS_PERSON")
                        .hasLabel("PERSON")
                        .out("PERSON")
                        .out("PERSON")
                        .has("name", P.within("BasketballPlayer")))
                .select("friend")
                .groupCount()
                .unfold()
                .order()
                .by(Column.values, Order.asc)
                .limit(20);
    }
}              
               
