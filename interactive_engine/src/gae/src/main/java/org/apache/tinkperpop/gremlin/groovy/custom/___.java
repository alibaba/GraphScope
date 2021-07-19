package org.apache.tinkperpop.gremlin.groovy.custom;
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * An anonymous {@link GraphTraversal}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ___ {

    protected ___() {
    }

    public static <A> CustomGraphTraversal<A, A> start() {
        return new CustomGraphTraversal<>();
    }

    public static <A> GraphTraversal<A, A> ___(final A... starts) {
        return inject(starts);
    }

    ///////////////////// MAP STEPS /////////////////////


    public static <A, B> GraphTraversal<A, String> expr(final String expression) {
        return ___.<A>start().expr(expression);
    }

    /**
     * @see GraphTraversal#map(Function)
     */
    public static <A, B> GraphTraversal<A, B> map(final Function<Traverser<A>, B> function) {
        return ___.<A>start().map(function);
    }

    /**
     * @see GraphTraversal#map(Traversal)
     */
    public static <A, B> GraphTraversal<A, B> map(final Traversal<?, B> mapTraversal) {
        return ___.<A>start().map(mapTraversal);
    }

    /**
     * @see GraphTraversal#flatMap(Function)
     */
    public static <A, B> GraphTraversal<A, B> flatMap(final Function<Traverser<A>, Iterator<B>> function) {
        return ___.<A>start().flatMap(function);
    }

    /**
     * @see GraphTraversal#flatMap(Traversal)
     */
    public static <A, B> GraphTraversal<A, B> flatMap(final Traversal<?, B> flatMapTraversal) {
        return ___.<A>start().flatMap(flatMapTraversal);
    }

    /**
     * @see GraphTraversal#identity()
     */
    public static <A> GraphTraversal<A, A> identity() {
        return ___.<A>start().identity();
    }

    /**
     * @see GraphTraversal#constant(Object)
     */
    public static <A> GraphTraversal<A, A> constant(final A a) {
        return ___.<A>start().constant(a);
    }

    /**
     * @see GraphTraversal#label()
     */
    public static <A extends Element> GraphTraversal<A, String> label() {
        return ___.<A>start().label();
    }

    /**
     * @see GraphTraversal#id()
     */
    public static <A extends Element> GraphTraversal<A, Object> id() {
        return ___.<A>start().id();
    }

    /**
     * @see GraphTraversal#V(Object...)
     */
    public static <A> GraphTraversal<A, Vertex> V(final Object... vertexIdsOrElements) {
        return ___.<A>start().V(vertexIdsOrElements);
    }

    /**
     * @see GraphTraversal#to(Direction, String...)
     */
    public static GraphTraversal<Vertex, Vertex> to(final Direction direction, final String... edgeLabels) {
        return ___.<Vertex>start().to(direction, edgeLabels);
    }

    /**
     * @see GraphTraversal#out(String...)
     */
    public static GraphTraversal<Vertex, Vertex> out(final String... edgeLabels) {
        return ___.<Vertex>start().out(edgeLabels);
    }

    /**
     * @see GraphTraversal#in(String...)
     */
    public static GraphTraversal<Vertex, Vertex> in(final String... edgeLabels) {
        return ___.<Vertex>start().in(edgeLabels);
    }

    /**
     * @see GraphTraversal#both(String...)
     */
    public static GraphTraversal<Vertex, Vertex> both(final String... edgeLabels) {
        return ___.<Vertex>start().both(edgeLabels);
    }

    /**
     * @see GraphTraversal#toE(Direction, String...)
     */
    public static GraphTraversal<Vertex, Edge> toE(final Direction direction, final String... edgeLabels) {
        return ___.<Vertex>start().toE(direction, edgeLabels);
    }

    /**
     * @see GraphTraversal#outE(String...)
     */
    public static GraphTraversal<Vertex, Edge> outE(final String... edgeLabels) {
        return ___.<Vertex>start().outE(edgeLabels);
    }

    /**
     * @see GraphTraversal#inE(String...)
     */
    public static GraphTraversal<Vertex, Edge> inE(final String... edgeLabels) {
        return ___.<Vertex>start().inE(edgeLabels);
    }

    /**
     * @see GraphTraversal#bothE(String...)
     */
    public static GraphTraversal<Vertex, Edge> bothE(final String... edgeLabels) {
        return ___.<Vertex>start().bothE(edgeLabels);
    }

    /**
     * @see GraphTraversal#toV(Direction)
     */
    public static GraphTraversal<Edge, Vertex> toV(final Direction direction) {
        return ___.<Edge>start().toV(direction);
    }

    /**
     * @see GraphTraversal#inV()
     */
    public static GraphTraversal<Edge, Vertex> inV() {
        return ___.<Edge>start().inV();
    }

    /**
     * @see GraphTraversal#outV()
     */
    public static GraphTraversal<Edge, Vertex> outV() {
        return ___.<Edge>start().outV();
    }

    /**
     * @see GraphTraversal#bothV()
     */
    public static GraphTraversal<Edge, Vertex> bothV() {
        return ___.<Edge>start().bothV();
    }

    /**
     * @see GraphTraversal#otherV()
     */
    public static GraphTraversal<Edge, Vertex> otherV() {
        return ___.<Edge>start().otherV();
    }

    /**
     * @see GraphTraversal#order()
     */
    public static <A> GraphTraversal<A, A> order() {
        return ___.<A>start().order();
    }

    /**
     * @see GraphTraversal#order(Scope)
     */
    public static <A> GraphTraversal<A, A> order(final Scope scope) {
        return ___.<A>start().order(scope);
    }

    /**
     * @see GraphTraversal#properties(String...)
     */
    public static <A extends Element, B> GraphTraversal<A, ? extends Property<B>> properties(final String... propertyKeys) {
        return ___.<A>start().<B>properties(propertyKeys);
    }

    /**
     * @see GraphTraversal#values(String...)
     */
    public static <A extends Element, B> GraphTraversal<A, B> values(final String... propertyKeys) {
        return ___.<A>start().values(propertyKeys);
    }

    /**
     * @see GraphTraversal#propertyMap(String...)
     */
    public static <A extends Element, B> GraphTraversal<A, Map<String, B>> propertyMap(final String... propertyKeys) {
        return ___.<A>start().propertyMap(propertyKeys);
    }

    /**
     * @see GraphTraversal#valueMap(String...)
     */
    public static <A extends Element, B> GraphTraversal<A, Map<Object, B>> valueMap(final String... propertyKeys) {
        return ___.<A>start().valueMap(propertyKeys);
    }

    /**
     * @see GraphTraversal#valueMap(boolean, String...)
     * @deprecated As of release 3.4.0, deprecated in favor of {@link org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__#valueMap(String...)} in conjunction with
     *             {@link GraphTraversal#with(String, Object)}.
     */
    @Deprecated
    public static <A extends Element, B> GraphTraversal<A, Map<Object, B>> valueMap(final boolean includeTokens, final String... propertyKeys) {
        return ___.<A>start().valueMap(includeTokens, propertyKeys);
    }

    /**
     * @see GraphTraversal#project(String, String...)
     */
    public static <A, B> GraphTraversal<A, Map<String, B>> project(final String projectKey, final String... projectKeys) {
        return ___.<A>start().project(projectKey, projectKeys);
    }

    /**
     * @see GraphTraversal#select(Column)
     */
    public static <A, B> GraphTraversal<A, Collection<B>> select(final Column column) {
        return ___.<A>start().select(column);
    }

    /**
     * @see GraphTraversal#key()
     */
    public static <A extends Property> GraphTraversal<A, String> key() {
        return ___.<A>start().key();
    }

    /**
     * @see GraphTraversal#value()
     */
    public static <A extends Property, B> GraphTraversal<A, B> value() {
        return ___.<A>start().value();
    }

    /**
     * @see GraphTraversal#path()
     */
    public static <A> GraphTraversal<A, Path> path() {
        return ___.<A>start().path();
    }

    /**
     * @see GraphTraversal#match(Traversal[])
     */
    public static <A, B> GraphTraversal<A, Map<String, B>> match(final Traversal<?, ?>... matchTraversals) {
        return ___.<A>start().match(matchTraversals);
    }

    /**
     * @see GraphTraversal#sack()
     */
    public static <A, B> GraphTraversal<A, B> sack() {
        return ___.<A>start().sack();
    }

    /**
     * @see GraphTraversal#loops()
     */
    public static <A> GraphTraversal<A, Integer> loops() {
        return ___.<A>start().loops();
    }

    /**
     * @see GraphTraversal#loops(String)
     */
    public static <A> GraphTraversal<A, Integer> loops(final String loopName) {
        return ___.<A>start().loops(loopName);
    }

    /**
     * @see GraphTraversal#select(Pop, String)
     */
    public static <A, B> GraphTraversal<A, B> select(final Pop pop, final String selectKey) {
        return ___.<A>start().select(pop, selectKey);
    }

    /**
     * @see GraphTraversal#select(String)
     */
    public static <A, B> GraphTraversal<A, B> select(final String selectKey) {
        return ___.<A>start().select(selectKey);
    }

    /**
     * @see GraphTraversal#select(Pop, String, String, String...)
     */
    public static <A, B> GraphTraversal<A, Map<String, B>> select(final Pop pop, final String selectKey1, final String selectKey2, final String... otherSelectKeys) {
        return ___.<A>start().select(pop, selectKey1, selectKey2, otherSelectKeys);
    }

    /**
     * @see GraphTraversal#select(String, String, String...)
     */
    public static <A, B> GraphTraversal<A, Map<String, B>> select(final String selectKey1, final String selectKey2, final String... otherSelectKeys) {
        return ___.<A>start().select(selectKey1, selectKey2, otherSelectKeys);
    }

    /**
     * @see GraphTraversal#select(Pop, Traversal)
     */
    public static <A, B> GraphTraversal<A, B> select(final Pop pop, final Traversal<A, B> keyTraversal) {
        return ___.<A>start().select(pop, keyTraversal);
    }

    /**
     * @see GraphTraversal#select(Traversal)
     */
    public static <A, B> GraphTraversal<A, B> select(final Traversal<A, B> keyTraversal) {
        return ___.<A>start().select(keyTraversal);
    }

    /**
     * @see GraphTraversal#unfold()
     */
    public static <A> GraphTraversal<A, A> unfold() {
        return ___.<A>start().unfold();
    }

    /**
     * @see GraphTraversal#fold()
     */
    public static <A> GraphTraversal<A, List<A>> fold() {
        return ___.<A>start().fold();
    }

    /**
     * @see GraphTraversal#fold(Object, BiFunction)
     */
    public static <A, B> GraphTraversal<A, B> fold(final B seed, final BiFunction<B, A, B> foldFunction) {
        return ___.<A>start().fold(seed, foldFunction);
    }

    /**
     * @see GraphTraversal#count()
     */
    public static <A> GraphTraversal<A, Long> count() {
        return ___.<A>start().count();
    }

    /**
     * @see GraphTraversal#count(Scope)
     */
    public static <A> GraphTraversal<A, Long> count(final Scope scope) {
        return ___.<A>start().count(scope);
    }

    /**
     * @see GraphTraversal#sum()
     */
    public static <A> GraphTraversal<A, Double> sum() {
        return ___.<A>start().sum();
    }

    /**
     * @see GraphTraversal#sum(Scope)
     */
    public static <A> GraphTraversal<A, Double> sum(final Scope scope) {
        return ___.<A>start().sum(scope);
    }

    /**
     * @see GraphTraversal#min()
     */
    public static <A, B extends Comparable> GraphTraversal<A, B> min() {
        return ___.<A>start().min();
    }

    /**
     * @see GraphTraversal#min(Scope)
     */
    public static <A, B extends Comparable> GraphTraversal<A, B> min(final Scope scope) {
        return ___.<A>start().min(scope);
    }

    /**
     * @see GraphTraversal#max()
     */
    public static <A, B extends Comparable> GraphTraversal<A, B> max() {
        return ___.<A>start().max();
    }

    /**
     * @see GraphTraversal#max(Scope)
     */
    public static <A, B extends Comparable> GraphTraversal<A, B> max(final Scope scope) {
        return ___.<A>start().max(scope);
    }

    /**
     * @see GraphTraversal#mean()
     */
    public static <A> GraphTraversal<A, Double> mean() {
        return ___.<A>start().mean();
    }

    /**
     * @see GraphTraversal#mean(Scope)
     */
    public static <A> GraphTraversal<A, Double> mean(final Scope scope) {
        return ___.<A>start().mean(scope);
    }

    /**
     * @see GraphTraversal#group()
     */
    public static <A, K, V> GraphTraversal<A, Map<K, V>> group() {
        return ___.<A>start().group();
    }

    /**
     * @see GraphTraversal#groupCount()
     */
    public static <A, K> GraphTraversal<A, Map<K, Long>> groupCount() {
        return ___.<A>start().<K>groupCount();
    }

    /**
     * @see GraphTraversal#tree()
     */
    public static <A> GraphTraversal<A, Tree> tree() {
        return ___.<A>start().tree();
    }

    /**
     * @see GraphTraversal#addV(String)
     */
    public static <A> GraphTraversal<A, Vertex> addV(final String vertexLabel) {
        return ___.<A>start().addV(vertexLabel);
    }

    /**
     * @see GraphTraversal#addV(org.apache.tinkerpop.gremlin.process.traversal.Traversal)
     */
    public static <A> GraphTraversal<A, Vertex> addV(final Traversal<?, String> vertexLabelTraversal) {
        return ___.<A>start().addV(vertexLabelTraversal);
    }

    /**
     * @see GraphTraversal#addV()
     */
    public static <A> GraphTraversal<A, Vertex> addV() {
        return ___.<A>start().addV();
    }

    /**
     * @see GraphTraversal#addE(String)
     */
    public static <A> GraphTraversal<A, Edge> addE(final String edgeLabel) {
        return ___.<A>start().addE(edgeLabel);
    }

    /**
     * @see GraphTraversal#addE(org.apache.tinkerpop.gremlin.process.traversal.Traversal)
     */
    public static <A> GraphTraversal<A, Edge> addE(final Traversal<?, String> edgeLabelTraversal) {
        return ___.<A>start().addE(edgeLabelTraversal);
    }

    /**
     * @see GraphTraversal#math(String)
     */
    public static <A> GraphTraversal<A, Double> math(final String expression) {
        return ___.<A>start().math(expression);
    }

    ///////////////////// FILTER STEPS /////////////////////

    /**
     * @see GraphTraversal#filter(Predicate)
     */
    public static <A> GraphTraversal<A, A> filter(final Predicate<Traverser<A>> predicate) {
        return ___.<A>start().filter(predicate);
    }

    /**
     * @see GraphTraversal#filter(Traversal)
     */
    public static <A> GraphTraversal<A, A> filter(final Traversal<?, ?> filterTraversal) {
        return ___.<A>start().filter(filterTraversal);
    }

    /**
     * @see GraphTraversal#and(Traversal[])
     */
    public static <A> GraphTraversal<A, A> and(final Traversal<?, ?>... andTraversals) {
        return ___.<A>start().and(andTraversals);
    }

    /**
     * @see GraphTraversal#or(Traversal[])
     */
    public static <A> GraphTraversal<A, A> or(final Traversal<?, ?>... orTraversals) {
        return ___.<A>start().or(orTraversals);
    }

    /**
     * @see GraphTraversal#inject(Object[])
     */
    public static <A> GraphTraversal<A, A> inject(final A... injections) {
        return ___.<A>start().inject((A[]) injections);
    }

    /**
     * @see GraphTraversal#dedup(String...)
     */
    public static <A> GraphTraversal<A, A> dedup(final String... dedupLabels) {
        return ___.<A>start().dedup(dedupLabels);
    }

    /**
     * @see GraphTraversal#dedup(Scope, String...)
     */
    public static <A> GraphTraversal<A, A> dedup(final Scope scope, final String... dedupLabels) {
        return ___.<A>start().dedup(scope, dedupLabels);
    }

    /**
     * @see GraphTraversal#has(String, P)
     */
    public static <A> GraphTraversal<A, A> has(final String propertyKey, final P<?> predicate) {
        return ___.<A>start().has(propertyKey, predicate);
    }

    /**
     * @see GraphTraversal#has(T, P)
     */
    public static <A> GraphTraversal<A, A> has(final T accessor, final P<?> predicate) {
        return ___.<A>start().has(accessor, predicate);
    }

    /**
     * @see GraphTraversal#has(String, Object)
     */
    public static <A> GraphTraversal<A, A> has(final String propertyKey, final Object value) {
        return ___.<A>start().has(propertyKey, value);
    }

    /**
     * @see GraphTraversal#has(T, Object)
     */
    public static <A> GraphTraversal<A, A> has(final T accessor, final Object value) {
        return ___.<A>start().has(accessor, value);
    }

    /**
     * @see GraphTraversal#has(String, String, Object)
     */
    public static <A> GraphTraversal<A, A> has(final String label, final String propertyKey, final Object value) {
        return ___.<A>start().has(label, propertyKey, value);
    }

    /**
     * @see GraphTraversal#has(String, String, P)
     */
    public static <A> GraphTraversal<A, A> has(final String label, final String propertyKey, final P<?> predicate) {
        return ___.<A>start().has(label, propertyKey, predicate);
    }

    /**
     * @see GraphTraversal#has(T, Traversal)
     */
    public static <A> GraphTraversal<A, A> has(final T accessor, final Traversal<?, ?> propertyTraversal) {
        return ___.<A>start().has(accessor, propertyTraversal);
    }

    /**
     * @see GraphTraversal#has(String, Traversal)
     */
    public static <A> GraphTraversal<A, A> has(final String propertyKey, final Traversal<?, ?> propertyTraversal) {
        return ___.<A>start().has(propertyKey, propertyTraversal);
    }

    /**
     * @see GraphTraversal#has(String)
     */
    public static <A> GraphTraversal<A, A> has(final String propertyKey) {
        return ___.<A>start().has(propertyKey);
    }

    /**
     * @see GraphTraversal#hasNot(String)
     */
    public static <A> GraphTraversal<A, A> hasNot(final String propertyKey) {
        return ___.<A>start().hasNot(propertyKey);
    }

    /**
     * @see GraphTraversal#hasLabel(String, String...)
     */
    public static <A> GraphTraversal<A, A> hasLabel(final String label, String... otherLabels) {
        return ___.<A>start().hasLabel(label, otherLabels);
    }

    /**
     * @see GraphTraversal#hasLabel(P)
     */
    public static <A> GraphTraversal<A, A> hasLabel(final P<String> predicate) {
        return ___.<A>start().hasLabel(predicate);
    }

    /**
     * @see GraphTraversal#hasId(Object, Object...)
     */
    public static <A> GraphTraversal<A, A> hasId(final Object id, Object... otherIds) {
        return ___.<A>start().hasId(id, otherIds);
    }

    /**
     * @see GraphTraversal#hasId(P)
     */
    public static <A> GraphTraversal<A, A> hasId(final P<Object> predicate) {
        return ___.<A>start().hasId(predicate);
    }

    /**
     * @see GraphTraversal#hasKey(String, String...)
     */
    public static <A> GraphTraversal<A, A> hasKey(final String label, String... otherLabels) {
        return ___.<A>start().hasKey(label, otherLabels);
    }

    /**
     * @see GraphTraversal#hasKey(P)
     */
    public static <A> GraphTraversal<A, A> hasKey(final P<String> predicate) {
        return ___.<A>start().hasKey(predicate);
    }

    /**
     * @see GraphTraversal#hasValue(Object, Object...)
     */
    public static <A> GraphTraversal<A, A> hasValue(final Object value, Object... values) {
        return ___.<A>start().hasValue(value, values);
    }

    /**
     * @see GraphTraversal#hasValue(P)
     */
    public static <A> GraphTraversal<A, A> hasValue(final P<Object> predicate) {
        return ___.<A>start().hasValue(predicate);
    }

    /**
     * @see GraphTraversal#where(String, P)
     */
    public static <A> GraphTraversal<A, A> where(final String startKey, final P<String> predicate) {
        return ___.<A>start().where(startKey, predicate);
    }

    /**
     * @see GraphTraversal#where(P)
     */
    public static <A> GraphTraversal<A, A> where(final P<String> predicate) {
        return ___.<A>start().where(predicate);
    }

    /**
     * @see GraphTraversal#where(Traversal)
     */
    public static <A> GraphTraversal<A, A> where(final Traversal<?, ?> whereTraversal) {
        return ___.<A>start().where(whereTraversal);
    }

    /**
     * @see GraphTraversal#is(P)
     */
    public static <A> GraphTraversal<A, A> is(final P<A> predicate) {
        return ___.<A>start().is(predicate);
    }

    /**
     * @see GraphTraversal#is(Object)
     */
    public static <A> GraphTraversal<A, A> is(final Object value) {
        return ___.<A>start().is(value);
    }

    /**
     * @see GraphTraversal#not(Traversal)
     */
    public static <A> GraphTraversal<A, A> not(final Traversal<?, ?> notTraversal) {
        return ___.<A>start().not(notTraversal);
    }

    /**
     * @see GraphTraversal#coin(double)
     */
    public static <A> GraphTraversal<A, A> coin(final double probability) {
        return ___.<A>start().coin(probability);
    }

    /**
     * @see GraphTraversal#range(long, long)
     */
    public static <A> GraphTraversal<A, A> range(final long low, final long high) {
        return ___.<A>start().range(low, high);
    }

    /**
     * @see GraphTraversal#range(Scope, long, long)
     */
    public static <A> GraphTraversal<A, A> range(final Scope scope, final long low, final long high) {
        return ___.<A>start().range(scope, low, high);
    }

    /**
     * @see GraphTraversal#limit(long)
     */
    public static <A> GraphTraversal<A, A> limit(final long limit) {
        return ___.<A>start().limit(limit);
    }

    /**
     * @see GraphTraversal#limit(Scope, long)
     */
    public static <A> GraphTraversal<A, A> limit(final Scope scope, final long limit) {
        return ___.<A>start().limit(scope, limit);
    }

    /**
     * @see GraphTraversal#skip(long)
     */
    public static <A> GraphTraversal<A, A> skip(final long skip) {
        return ___.<A>start().skip(skip);
    }

    /**
     * @see GraphTraversal#skip(Scope, long)
     */
    public static <A> GraphTraversal<A, A> skip(final Scope scope, final long skip) {
        return ___.<A>start().skip(scope, skip);
    }

    /**
     * @see GraphTraversal#tail()
     */
    public static <A> GraphTraversal<A, A> tail() {
        return ___.<A>start().tail();
    }

    /**
     * @see GraphTraversal#tail(long)
     */
    public static <A> GraphTraversal<A, A> tail(final long limit) {
        return ___.<A>start().tail(limit);
    }

    /**
     * @see GraphTraversal#tail(Scope)
     */
    public static <A> GraphTraversal<A, A> tail(final Scope scope) {
        return ___.<A>start().tail(scope);
    }

    /**
     * @see GraphTraversal#tail(Scope, long)
     */
    public static <A> GraphTraversal<A, A> tail(final Scope scope, final long limit) {
        return ___.<A>start().tail(scope, limit);
    }

    /**
     * @see GraphTraversal#simplePath()
     */
    public static <A> GraphTraversal<A, A> simplePath() {
        return ___.<A>start().simplePath();
    }

    /**
     * @see GraphTraversal#cyclicPath()
     */
    public static <A> GraphTraversal<A, A> cyclicPath() {
        return ___.<A>start().cyclicPath();
    }

    /**
     * @see GraphTraversal#sample(int)
     */
    public static <A> GraphTraversal<A, A> sample(final int amountToSample) {
        return ___.<A>start().sample(amountToSample);
    }

    /**
     * @see GraphTraversal#sample(Scope, int)
     */
    public static <A> GraphTraversal<A, A> sample(final Scope scope, final int amountToSample) {
        return ___.<A>start().sample(scope, amountToSample);
    }

    /**
     * @see GraphTraversal#drop()
     */
    public static <A> GraphTraversal<A, A> drop() {
        return ___.<A>start().drop();
    }

    ///////////////////// SIDE-EFFECT STEPS /////////////////////

    /**
     * @see GraphTraversal#sideEffect(Consumer)
     */
    public static <A> GraphTraversal<A, A> sideEffect(final Consumer<Traverser<A>> consumer) {
        return ___.<A>start().sideEffect(consumer);
    }

    /**
     * @see GraphTraversal#sideEffect(Traversal)
     */
    public static <A> GraphTraversal<A, A> sideEffect(final Traversal<?, ?> sideEffectTraversal) {
        return ___.<A>start().sideEffect(sideEffectTraversal);
    }

    /**
     * @see GraphTraversal#cap(String, String...)
     */
    public static <A, B> GraphTraversal<A, B> cap(final String sideEffectKey, String... sideEffectKeys) {
        return ___.<A>start().cap(sideEffectKey, sideEffectKeys);
    }

    /**
     * @see GraphTraversal#subgraph(String)
     */
    public static <A> GraphTraversal<A, Edge> subgraph(final String sideEffectKey) {
        return ___.<A>start().subgraph(sideEffectKey);
    }

    /**
     * @see GraphTraversal#aggregate(String)
     */
    public static <A> GraphTraversal<A, A> aggregate(final String sideEffectKey) {
        return ___.<A>start().aggregate(sideEffectKey);
    }

    /**
     * @see GraphTraversal#group(String)
     */
    public static <A> GraphTraversal<A, A> group(final String sideEffectKey) {
        return ___.<A>start().group(sideEffectKey);
    }

    /**
     * @see GraphTraversal#groupCount(String)
     */
    public static <A> GraphTraversal<A, A> groupCount(final String sideEffectKey) {
        return ___.<A>start().groupCount(sideEffectKey);
    }

    /**
     * @see GraphTraversal#timeLimit(long)
     */
    public static <A> GraphTraversal<A, A> timeLimit(final long timeLimit) {
        return ___.<A>start().timeLimit(timeLimit);
    }

    /**
     * @see GraphTraversal#tree(String)
     */
    public static <A> GraphTraversal<A, A> tree(final String sideEffectKey) {
        return ___.<A>start().tree(sideEffectKey);
    }

    /**
     * @see GraphTraversal#sack(BiFunction)
     */
    public static <A, V, U> GraphTraversal<A, A> sack(final BiFunction<V, U, V> sackOperator) {
        return ___.<A>start().sack(sackOperator);
    }

    /**
     * @see GraphTraversal#store(String)
     */
    public static <A> GraphTraversal<A, A> store(final String sideEffectKey) {
        return ___.<A>start().store(sideEffectKey);
    }

    /**
     * @see GraphTraversal#property(Object, Object, Object...)
     */
    public static <A> GraphTraversal<A, A> property(final Object key, final Object value, final Object... keyValues) {
        return ___.<A>start().property(key, value, keyValues);
    }

    /**
     * @see GraphTraversal#property(VertexProperty.Cardinality, Object, Object, Object...)
     */
    public static <A> GraphTraversal<A, A> property(final VertexProperty.Cardinality cardinality, final Object key, final Object value, final Object... keyValues) {
        return ___.<A>start().property(cardinality, key, value, keyValues);
    }

    ///////////////////// BRANCH STEPS /////////////////////

    /**
     * @see GraphTraversal#branch(Function)
     */
    public static <A, M, B> GraphTraversal<A, B> branch(final Function<Traverser<A>, M> function) {
        return ___.<A>start().branch(function);
    }

    /**
     * @see GraphTraversal#branch(Traversal)
     */
    public static <A, M, B> GraphTraversal<A, B> branch(final Traversal<?, M> traversalFunction) {
        return ___.<A>start().branch(traversalFunction);
    }

    /**
     * @see GraphTraversal#choose(Predicate, Traversal, Traversal)
     */
    public static <A, B> GraphTraversal<A, B> choose(final Predicate<A> choosePredicate, final Traversal<?, B> trueChoice, final Traversal<?, B> falseChoice) {
        return ___.<A>start().choose(choosePredicate, trueChoice, falseChoice);
    }

    /**
     * @see GraphTraversal#choose(Predicate, Traversal)
     */
    public static <A, B> GraphTraversal<A, B> choose(final Predicate<A> choosePredicate, final Traversal<?, B> trueChoice) {
        return ___.<A>start().choose(choosePredicate, trueChoice);
    }

    /**
     * @see GraphTraversal#choose(Function)
     */
    public static <A, M, B> GraphTraversal<A, B> choose(final Function<A, M> choiceFunction) {
        return ___.<A>start().choose(choiceFunction);
    }

    /**
     * @see GraphTraversal#choose(Traversal)
     */
    public static <A, M, B> GraphTraversal<A, B> choose(final Traversal<?, M> traversalFunction) {
        return ___.<A>start().choose(traversalFunction);
    }

    /**
     * @see GraphTraversal#choose(Traversal, Traversal, Traversal)
     */
    public static <A, M, B> GraphTraversal<A, B> choose(final Traversal<?, M> traversalPredicate, final Traversal<?, B> trueChoice, final Traversal<?, B> falseChoice) {
        return ___.<A>start().choose(traversalPredicate, trueChoice, falseChoice);
    }

    /**
     * @see GraphTraversal#choose(Traversal, Traversal)
     */
    public static <A, M, B> GraphTraversal<A, B> choose(final Traversal<?, M> traversalPredicate, final Traversal<?, B> trueChoice) {
        return ___.<A>start().choose(traversalPredicate, trueChoice);
    }

    /**
     * @see GraphTraversal#optional(Traversal)
     */
    public static <A> GraphTraversal<A, A> optional(final Traversal<?, A> optionalTraversal) {
        return ___.<A>start().optional(optionalTraversal);
    }

    /**
     * @see GraphTraversal#union(Traversal[])
     */
    public static <A, B> GraphTraversal<A, B> union(final Traversal<?, B>... traversals) {
        return ___.<A>start().union(traversals);
    }

    /**
     * @see GraphTraversal#coalesce(Traversal[])
     */
    public static <A, B> GraphTraversal<A, B> coalesce(final Traversal<?, B>... traversals) {
        return ___.<A>start().coalesce(traversals);
    }

    /**
     * @see GraphTraversal#repeat(Traversal)
     */
    public static <A> GraphTraversal<A, A> repeat(final Traversal<?, A> traversal) {
        return ___.<A>start().repeat(traversal);
    }

    /**
     * @see GraphTraversal#repeat(Traversal)
     */
    public static <A> GraphTraversal<A, A> repeat(final String loopName, final Traversal<?, A> traversal) {
        return ___.<A>start().repeat(loopName, traversal);
    }

    /**
     * @see GraphTraversal#emit(Traversal)
     */
    public static <A> GraphTraversal<A, A> emit(final Traversal<?, ?> emitTraversal) {
        return ___.<A>start().emit(emitTraversal);
    }

    /**
     * @see GraphTraversal#emit(Predicate)
     */
    public static <A> GraphTraversal<A, A> emit(final Predicate<Traverser<A>> emitPredicate) {
        return ___.<A>start().emit(emitPredicate);
    }

    /**
     * @see GraphTraversal#until(Traversal)
     */
    public static <A> GraphTraversal<A, A> until(final Traversal<?, ?> untilTraversal) {
        return ___.<A>start().until(untilTraversal);
    }

    /**
     * @see GraphTraversal#until(Predicate)
     */
    public static <A> GraphTraversal<A, A> until(final Predicate<Traverser<A>> untilPredicate) {
        return ___.<A>start().until(untilPredicate);
    }

    /**
     * @see GraphTraversal#times(int)
     */
    public static <A> GraphTraversal<A, A> times(final int maxLoops) {
        return ___.<A>start().times(maxLoops);
    }

    /**
     * @see GraphTraversal#emit()
     */
    public static <A> GraphTraversal<A, A> emit() {
        return ___.<A>start().emit();
    }

    /**
     * @see GraphTraversal#local(Traversal)
     */
    public static <A, B> GraphTraversal<A, B> local(final Traversal<?, B> localTraversal) {
        return ___.<A>start().local(localTraversal);
    }

    ///////////////////// UTILITY STEPS /////////////////////

    /**
     * @see GraphTraversal#as(String, String...)
     */
    public static <A> GraphTraversal<A, A> as(final String label, final String... labels) {
        return ___.<A>start().as(label, labels);
    }

    /**
     * @see GraphTraversal#barrier()
     */
    public static <A> GraphTraversal<A, A> barrier() {
        return ___.<A>start().barrier();
    }

    /**
     * @see GraphTraversal#barrier(int)
     */
    public static <A> GraphTraversal<A, A> barrier(final int maxBarrierSize) {
        return ___.<A>start().barrier(maxBarrierSize);
    }

    /**
     * @see GraphTraversal#barrier(Consumer)
     */
    public static <A> GraphTraversal<A, A> barrier(final Consumer<TraverserSet<Object>> barrierConsumer) {
        return ___.<A>start().barrier(barrierConsumer);
    }

    /**
     * @see GraphTraversal#index()
     */
    public static <A, B> GraphTraversal<A, B> index() {
        return ___.<A>start().index();
    }
}
