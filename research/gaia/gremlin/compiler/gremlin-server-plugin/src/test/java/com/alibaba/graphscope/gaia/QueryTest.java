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
package com.alibaba.graphscope.gaia;

import com.alibaba.graphscope.gaia.config.ExperimentalGaiaConfig;
import com.alibaba.graphscope.gaia.config.GaiaConfig;
import com.alibaba.graphscope.gaia.idmaker.IdMaker;
import com.alibaba.graphscope.gaia.idmaker.IncrementalQueryIdMaker;
import com.alibaba.graphscope.gaia.idmaker.TagIdMaker;
import com.alibaba.graphscope.gaia.plan.PlanUtils;
import com.alibaba.graphscope.gaia.processor.GaiaGraphOpProcessor;
import com.alibaba.graphscope.gaia.store.ExperimentalGraphStore;
import com.alibaba.graphscope.gaia.store.GraphStoreService;
import com.alibaba.pegasus.builder.AbstractBuilder;
import com.alibaba.graphscope.gaia.plan.translator.TraversalTranslator;
import com.alibaba.graphscope.gaia.plan.translator.builder.PlanConfig;
import com.alibaba.graphscope.gaia.plan.translator.builder.TraversalBuilder;
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

public class QueryTest {
    private static Logger logger = LoggerFactory.getLogger(QueryTest.class);
    private static final GraphTraversalSource g = EmptyGraph.instance().traversal();

    public static void main(String[] args) {
        GaiaConfig config = new ExperimentalGaiaConfig("conf");
        GraphStoreService graphStore = new ExperimentalGraphStore(config);
        IdMaker queryIdMaker = new IncrementalQueryIdMaker();
        test_CR_1_1(config, graphStore, queryIdMaker);
        test_CR_1_2(config, graphStore, queryIdMaker);
        test_CR_2(config, graphStore, queryIdMaker);
        test_CR_3_1(config, graphStore, queryIdMaker);
        test_CR_3_2(config, graphStore, queryIdMaker);
        test_CR_5(config, graphStore, queryIdMaker);
        test_CR_6(config, graphStore, queryIdMaker);
        test_CR_7(config, graphStore, queryIdMaker);
        test_CR_8(config, graphStore, queryIdMaker);
        test_CR_9(config, graphStore, queryIdMaker);
        test_CR_11(config, graphStore, queryIdMaker);
        test_CR_12(config, graphStore, queryIdMaker);
    }

    public static void test_CR_1_1(GaiaConfig config, GraphStoreService storeService, IdMaker queryIdMaker) {
        Traversal testTraversal = CR_1_1();
        GaiaGraphOpProcessor.applyStrategy(testTraversal, config, storeService);
        long queryId = (long) queryIdMaker.getId(testTraversal.asAdmin());
        AbstractBuilder job = new TraversalTranslator((new TraversalBuilder(testTraversal.asAdmin()))
                .addConfig(PlanConfig.QUERY_ID, queryId)
                .addConfig(PlanConfig.TAG_ID_MAKER, new TagIdMaker(testTraversal.asAdmin()))
                .addConfig(PlanConfig.QUERY_CONFIG, PlanUtils.getDefaultConfig(queryId, config))).translate();
        PlanUtils.print(job);
    }

    public static Traversal CR_1_1() {
        return g.V().hasLabel("PERSON").has("id", 30786325583618L)
                .out()
                .both("HASCREATOR")
                .union(__.identity(),
                        __.both("HASCREATOR").union(__.identity(), __.both("HASCREATOR")))
                .has("id", P.neq(30786325583618L)).has("firstName", P.eq("Chau")).as("a")
                .path().count(Scope.local)
                .as("b")
                .select("a")
                .order().by(__.select("b"), Order.desc).by("lastName").by("location")
                .limit(20)
                .select("a", "b");
    }

    public static void test_CR_1_2(GaiaConfig config, GraphStoreService storeService, IdMaker queryIdMaker) {
        Traversal testTraversal = CR_1_2();
        GaiaGraphOpProcessor.applyStrategy(testTraversal, config, storeService);
        long queryId = (long) queryIdMaker.getId(testTraversal.asAdmin());
        AbstractBuilder job = new TraversalTranslator((new TraversalBuilder(testTraversal.asAdmin()))
                .addConfig(PlanConfig.QUERY_ID, queryId)
                .addConfig(PlanConfig.TAG_ID_MAKER, new TagIdMaker(testTraversal.asAdmin()))
                .addConfig(PlanConfig.QUERY_CONFIG, PlanUtils.getDefaultConfig(queryId, config))).translate();
        PlanUtils.print(job);
    }

    public static Traversal CR_1_2() {
        return g.V(1).union(
                __.out("HASCREATOR").values("name"),
                __.outE("HASCREATOR").as("study").inV().as("u").out("HASCREATOR").as("city")
                        .select("study", "u", "city"),
                __.outE("HASCREATOR").as("we").inV().as("company").out("HASCREATOR").as("country")
                        .select("we", "company", "country"));
    }

    public static void test_CR_2(GaiaConfig config, GraphStoreService storeService, IdMaker queryIdMaker) {
        Traversal testTraversal = CR_2();
        GaiaGraphOpProcessor.applyStrategy(testTraversal, config, storeService);
        long queryId = (long) queryIdMaker.getId(testTraversal.asAdmin());
        AbstractBuilder job = new TraversalTranslator((new TraversalBuilder(testTraversal.asAdmin()))
                .addConfig(PlanConfig.QUERY_ID, queryId)
                .addConfig(PlanConfig.TAG_ID_MAKER, new TagIdMaker(testTraversal.asAdmin()))
                .addConfig(PlanConfig.QUERY_CONFIG, PlanUtils.getDefaultConfig(queryId, config))).translate();
        PlanUtils.print(job);
    }

    public static Traversal CR_2() {
        return g.V().hasLabel("PERSON").has("id", 17592186052613L)
                .both("HASCREATOR").as("p")
                .in("HASCREATOR").has("creationDate", P.lte(20121128000000000L))
                .order().by("creationDate", Order.desc).by("id", Order.asc)
                .limit(20).as("m")
                .select("p", "m")
                .by(__.valueMap("id", "firstName", "lastName"))
                .by(__.valueMap("id", "imageFile", "creationDate", "content"));
    }

    public static void test_CR_3_1(GaiaConfig config, GraphStoreService storeService, IdMaker queryIdMaker) {
        Traversal testTraversal = CR_3_1();
        GaiaGraphOpProcessor.applyStrategy(testTraversal, config, storeService);
        long queryId = (long) queryIdMaker.getId(testTraversal.asAdmin());
        AbstractBuilder job = new TraversalTranslator((new TraversalBuilder(testTraversal.asAdmin()))
                .addConfig(PlanConfig.QUERY_ID, queryId)
                .addConfig(PlanConfig.TAG_ID_MAKER, new TagIdMaker(testTraversal.asAdmin()))
                .addConfig(PlanConfig.QUERY_CONFIG, PlanUtils.getDefaultConfig(queryId, config))).translate();
        PlanUtils.print(job);
    }

    public static Traversal CR_3_1() {
        return g.V().hasLabel("PERSON").has("id", 17592186055119L)
                .both("HASCREATOR")
                .union(__.identity(), __.both("HASCREATOR"))
                .filter(__.out("HASCREATOR")
                        .has("name", P.without("United_Kingdom", "United_States")))
                .filter(__.in("HASCREATOR")
                        .has("creationDate", P.inside(20110601000000000L, 20111013000000000L))
                        .out("HASCREATOR").has("name", P.eq("United_Kingdom").or(P.eq("United_States"))))
                .group()
                .by()
                .by(__.in("HASCREATOR")
                        .has("creationDate", P.inside(20110601000000000L, 20111013000000000L))
                        .out("HASCREATOR")
                        .has("name", "United_Kingdom").count())
                .unfold()
                .order().by(__.select(Column.values), Order.desc).by(__.select(Column.keys).values("id"))
                .limit(20);
    }

    public static void test_CR_3_2(GaiaConfig config, GraphStoreService storeService, IdMaker queryIdMaker) {
        Traversal testTraversal = CR_3_2();
        GaiaGraphOpProcessor.applyStrategy(testTraversal, config, storeService);
        long queryId = (long) queryIdMaker.getId(testTraversal.asAdmin());
        AbstractBuilder job = new TraversalTranslator((new TraversalBuilder(testTraversal.asAdmin()))
                .addConfig(PlanConfig.QUERY_ID, queryId)
                .addConfig(PlanConfig.TAG_ID_MAKER, new TagIdMaker(testTraversal.asAdmin()))
                .addConfig(PlanConfig.QUERY_CONFIG, PlanUtils.getDefaultConfig(queryId, config))).translate();
        PlanUtils.print(job);
    }

    public static Traversal CR_3_2() {
        return g.V(1)
                .in("HASCREATOR")
                .has("creationDate", P.inside(20110601000000000L, 20110713000000000L))
                .filter(__.out("HASCREATOR").has("name", P.eq("United_States")))
                .count();
    }

    public static void test_CR_5(GaiaConfig config, GraphStoreService storeService, IdMaker queryIdMaker) {
        Traversal testTraversal = CR_5();
        GaiaGraphOpProcessor.applyStrategy(testTraversal, config, storeService);
        long queryId = (long) queryIdMaker.getId(testTraversal.asAdmin());
        AbstractBuilder job = new TraversalTranslator((new TraversalBuilder(testTraversal.asAdmin()))
                .addConfig(PlanConfig.QUERY_ID, queryId)
                .addConfig(PlanConfig.TAG_ID_MAKER, new TagIdMaker(testTraversal.asAdmin()))
                .addConfig(PlanConfig.QUERY_CONFIG, PlanUtils.getDefaultConfig(queryId, config))).translate();
        PlanUtils.print(job);
    }

    public static Traversal CR_5() {
        return g.V().hasLabel("PERSON").has("id", 21990232560302L)
                .both("HASCREATOR")
                .union(__.identity(), __.both("HASCREATOR"))
                .inE("HASCREATOR").has("joinDate", P.gt(20120901000000000L))
                .outV()
                .group()
                .by()
                .by(__.out("HASCREATOR").hasLabel("POST").out("HASCREATOR"))
                .unfold()
                .order()
                .by(__.select(Column.values), Order.desc)
                .by(__.select(Column.keys).values("id"), Order.asc)
                .limit(20);
    }

    public static void test_CR_6(GaiaConfig config, GraphStoreService storeService, IdMaker queryIdMaker) {
        Traversal testTraversal = CR_6();
        GaiaGraphOpProcessor.applyStrategy(testTraversal, config, storeService);
        long queryId = (long) queryIdMaker.getId(testTraversal.asAdmin());
        AbstractBuilder job = new TraversalTranslator((new TraversalBuilder(testTraversal.asAdmin()))
                .addConfig(PlanConfig.QUERY_ID, queryId)
                .addConfig(PlanConfig.TAG_ID_MAKER, new TagIdMaker(testTraversal.asAdmin()))
                .addConfig(PlanConfig.QUERY_CONFIG, PlanUtils.getDefaultConfig(queryId, config))).translate();
        PlanUtils.print(job);
    }

    public static Traversal CR_6() {
        return g.V().hasLabel("PERSON").has("id", 30786325583618L)
                .both("HASCREATOR").union(__.identity(), __.both("HASCREATOR"))
                .has("id", P.neq(30786325583618L))
                .in("HASCREATOR").hasLabel("POST")
                .filter(__.out("HASCREATOR").has("name", P.eq("Angola")))
                .out("HASCREATOR")
                .has("name", P.neq("HASCREATOR"))
                .groupCount()
                .unfold()
                .order()
                .by(__.select(Column.values), Order.desc)
                .by(__.select(Column.keys).values("age"), Order.asc)
                .limit(10);
    }

    public static void test_CR_7(GaiaConfig config, GraphStoreService storeService, IdMaker queryIdMaker) {
        Traversal testTraversal = CR_7();
        GaiaGraphOpProcessor.applyStrategy(testTraversal, config, storeService);
        long queryId = (long) queryIdMaker.getId(testTraversal.asAdmin());
        AbstractBuilder job = new TraversalTranslator((new TraversalBuilder(testTraversal.asAdmin()))
                .addConfig(PlanConfig.QUERY_ID, queryId)
                .addConfig(PlanConfig.TAG_ID_MAKER, new TagIdMaker(testTraversal.asAdmin()))
                .addConfig(PlanConfig.QUERY_CONFIG, PlanUtils.getDefaultConfig(queryId, config))).translate();
        PlanUtils.print(job);
    }

    public static Traversal CR_7() {
        return g.V().hasLabel("PERSON").has("id", 17592186053137L)
                .in("HASCREATOR").as("message")
                .inE("HASCREATOR").as("like")
                .values("creationDate")
                .as("likedate")
                .order().by(Order.asc).limit(10)
                .select("like")
                .outV().as("liker")
                .order()
                .by(__.select("likedate"), Order.desc)
                .by("location", Order.asc).limit(20)
                .select("message", "likedate", "liker")
                .by(__.valueMap("id", "content", "imageFile"))
                .by()
                .by(__.valueMap("id", "firstName", "lastName"));
    }

    public static void test_CR_8(GaiaConfig config, GraphStoreService storeService, IdMaker queryIdMaker) {
        Traversal testTraversal = CR_8();
        GaiaGraphOpProcessor.applyStrategy(testTraversal, config, storeService);
        long queryId = (long) queryIdMaker.getId(testTraversal.asAdmin());
        AbstractBuilder job = new TraversalTranslator((new TraversalBuilder(testTraversal.asAdmin()))
                .addConfig(PlanConfig.QUERY_ID, queryId)
                .addConfig(PlanConfig.TAG_ID_MAKER, new TagIdMaker(testTraversal.asAdmin()))
                .addConfig(PlanConfig.QUERY_CONFIG, PlanUtils.getDefaultConfig(queryId, config))).translate();
        PlanUtils.print(job);
    }

    public static Traversal CR_8() {
        return g.V().hasLabel("PERSON").has("id", 24189255818757L)
                .in("HASCREATOR")
                .in("HASCREATOR")
                .hasLabel("COMMENT").as("comment")
                .order().by("location", Order.desc).by("age", Order.asc)
                .limit(20)
                .out("HASCREATOR").as("commenter")
                .select("commenter", "comment")
                .by(__.valueMap("id", "firstName", "lastName"))
                .by(__.valueMap("creationDate", "id", "content"));
    }

    public static void test_CR_9(GaiaConfig config, GraphStoreService storeService, IdMaker queryIdMaker) {
        Traversal testTraversal = CR_9();
        GaiaGraphOpProcessor.applyStrategy(testTraversal, config, storeService);
        long queryId = (long) queryIdMaker.getId(testTraversal.asAdmin());
        AbstractBuilder job = new TraversalTranslator((new TraversalBuilder(testTraversal.asAdmin()))
                .addConfig(PlanConfig.QUERY_ID, queryId)
                .addConfig(PlanConfig.TAG_ID_MAKER, new TagIdMaker(testTraversal.asAdmin()))
                .addConfig(PlanConfig.QUERY_CONFIG, PlanUtils.getDefaultConfig(queryId, config))).translate();
        PlanUtils.print(job);
    }

    public static Traversal CR_9() {
        return g.V().hasLabel("PERSON").has("id", 13194139542834L)
                .both("HASCREATOR")
                .union(__.identity(), __.both("HASCREATOR"))
                .has("id", P.neq(13194139542834L)).as("friends")
                .in("HASCREATOR").has("creationDate", P.lt(20111217000000000L)).as("post")
                .order().by("creationDate", Order.desc).by("id", Order.asc).limit(20)
                .select("friends", "post")
                .by(__.valueMap("id", "firstName", "lastName"))
                .by(__.valueMap("id", "content", "imageFile", "creationDate"));
    }

    public static void test_CR_11(GaiaConfig config, GraphStoreService storeService, IdMaker queryIdMaker) {
        Traversal testTraversal = CR_11();
        GaiaGraphOpProcessor.applyStrategy(testTraversal, config, storeService);
        long queryId = (long) queryIdMaker.getId(testTraversal.asAdmin());
        AbstractBuilder job = new TraversalTranslator((new TraversalBuilder(testTraversal.asAdmin()))
                .addConfig(PlanConfig.QUERY_ID, queryId)
                .addConfig(PlanConfig.TAG_ID_MAKER, new TagIdMaker(testTraversal.asAdmin()))
                .addConfig(PlanConfig.QUERY_CONFIG, PlanUtils.getDefaultConfig(queryId, config))).translate();
        PlanUtils.print(job);
    }

    public static Traversal CR_11() {
        return g.V().hasLabel("PERSON").has("id", 30786325583618L).as("root")
                .both("HASCREATOR").union(__.identity(), __.both("HASCREATOR")).dedup()
                .has("id", P.neq(30786325583618L)).as("friends")
                .outE("HASCREATOR").has("workFrom", P.lt(2010)).as("startWork")
                .values("workFrom").as("works").select("startWork")
                .inV().as("comp")
                .values("name").as("orgname").select("comp")
                .out("HASCREATOR").has("name", "Laos")
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

    public static void test_CR_12(GaiaConfig config, GraphStoreService storeService, IdMaker queryIdMaker) {
        Traversal testTraversal = CR_12();
        GaiaGraphOpProcessor.applyStrategy(testTraversal, config, storeService);
        long queryId = (long) queryIdMaker.getId(testTraversal.asAdmin());
        AbstractBuilder job = new TraversalTranslator((new TraversalBuilder(testTraversal.asAdmin()))
                .addConfig(PlanConfig.QUERY_ID, queryId)
                .addConfig(PlanConfig.TAG_ID_MAKER, new TagIdMaker(testTraversal.asAdmin()))
                .addConfig(PlanConfig.QUERY_CONFIG, PlanUtils.getDefaultConfig(queryId, config))).translate();
        PlanUtils.print(job);
    }

    public static Traversal CR_12() {
        return g.V().hasLabel("PERSON").has("id", 17592186052613L)
                .both("HASCREATOR").as("friend").in("HASCREATOR").hasLabel("PERSON")
                .filter(__.out("HASCREATOR")
                        .hasLabel("PERSON")
                        .out("PERSON")
                        .out("PERSON")
                        .has("name", P.within("BasketballPlayer")))
                .select("friend")
                .groupCount()
                .unfold()
                .order()
                .by(Column.values, Order.asc).by(__.select(Column.keys).values("age"))
                .limit(20);
    }
}              
               
