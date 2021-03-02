/**
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
package com.alibaba.graphscope.utils;

import com.alibaba.graphscope.common.proto.Common;
import com.alibaba.graphscope.common.proto.Gremlin;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;

import java.util.List;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

import static com.alibaba.graphscope.utils.EncodeValue.*;

public class FilterHelper {
    public static FilterHelper INSTANCE = new FilterHelper();

    private FilterHelper() {
    }

    public Gremlin.FilterExp has(Common.Key key, Gremlin.Compare cmp, Common.Value value) {
        return Gremlin.FilterExp.newBuilder().setLeft(key)
                .setCmp(cmp)
                .setRight(value)
                .build();
    }

    // The id predicates:
    public Gremlin.FilterExp hasId(Gremlin.Compare cmp, Common.Value value) {
        Common.Key key = Common.Key.newBuilder().setId(Common.IdKey.newBuilder().build()).build();
        return has(key, cmp, value);
    }

    public Gremlin.FilterExp hasLabel(Gremlin.Compare cmp, Common.Value value) {
        Common.Key key = Common.Key.newBuilder().setLabel(Common.LabelKey.newBuilder().build()).build();
        return has(key, cmp, value);
    }

    public Gremlin.FilterExp hasProperty(String name, Gremlin.Compare cmp, Common.Value value) {
        Common.Key key = Common.Key.newBuilder()
                .setName(name)
                .build();
        return has(key, cmp, value);
    }

    public Gremlin.FilterExp hasProperty(int name, Gremlin.Compare cmp, Common.Value value) {
        Common.Key key = Common.Key.newBuilder()
                .setNameId(name)
                .build();
        return has(key, cmp, value);
    }

    public Gremlin.FilterExp idPredicate(final Number id, final BiPredicate predicate) {
        Gremlin.Compare compare = convertFromBiPredicate(predicate);
        if (id == null) {
            return hasId(compare, fromNull());
        } else if (id instanceof Long) {
            return hasId(compare, fromLong(id.longValue()));
        } else if (id instanceof Integer) {
            return hasId(compare, fromInt(id.intValue()));
        } else {
            throw new UnsupportedOperationException("number type not supported " + id.getClass());
        }
    }

    public static Gremlin.Compare convertFromBiPredicate(final BiPredicate predicate) {
        if (predicate == Compare.eq) {
            return Gremlin.Compare.EQ;
        } else if (predicate == Compare.neq) {
            return Gremlin.Compare.NE;
        } else if (predicate == Compare.lt) {
            return Gremlin.Compare.LT;
        } else if (predicate == Compare.lte) {
            return Gremlin.Compare.LE;
        } else if (predicate == Compare.gt) {
            return Gremlin.Compare.GT;
        } else if (predicate == Compare.gte) {
            return Gremlin.Compare.GE;
        } else if (predicate == Contains.within) {
            return Gremlin.Compare.WITHIN;
        } else if (predicate == Contains.without) {
            return Gremlin.Compare.WITHOUT;
        } else {
            throw new UnsupportedOperationException("cannot convert from " + predicate);
        }
    }

    public Gremlin.FilterExp labelPredicate(final Number id, final BiPredicate predicate) {
        Gremlin.Compare compare = convertFromBiPredicate(predicate);
        if (id == null) {
            return hasLabel(compare, fromNull());
        } else if (id instanceof Long) {
            return hasLabel(compare, fromLong(id.longValue()));
        } else if (id instanceof Integer) {
            return hasLabel(compare, fromInt(id.intValue()));
        } else {
            throw new UnsupportedOperationException("number type not supported " + id.getClass());
        }
    }

    public Gremlin.FilterExp propertyPredicate(final String name, final Number value, final BiPredicate predicate) {
        Gremlin.Compare compare = convertFromBiPredicate(predicate);
        if (value == null) {
            return hasProperty(name, compare, fromNull());
        } else if (value instanceof Long) {
            return hasProperty(name, compare, fromLong(value.longValue()));
        } else if (value instanceof Integer) {
            return hasProperty(name, compare, fromInt(value.intValue()));
        } else {
            throw new UnsupportedOperationException("number type not supported " + value.getClass());
        }
    }

    public Gremlin.FilterExp propertyPredicate(final String name, final List<Object> value, final BiPredicate predicate) {
        Gremlin.Compare compare = convertFromBiPredicate(predicate);
        if (value == null || value.isEmpty()) {
            return hasProperty(name, compare, fromNull());
        } else if (value.get(0) instanceof String) {
            return hasProperty(name, compare, fromStrArray(value.stream().map(k -> (String) k).collect(Collectors.toList())));
        } else if (value.get(0) instanceof Integer) {
            return hasProperty(name, compare, fromIntArray(value.stream().map(k -> (Integer) k).collect(Collectors.toList())));
        } else if (value.get(0) instanceof Long) {
            return hasProperty(name, compare, fromLongArray(value.stream().map(k -> (Long) k).collect(Collectors.toList())));
        } else {
            throw new UnsupportedOperationException("cannot support other list value type " + value.get(0).getClass());
        }
    }

    public Gremlin.FilterExp propertyPredicate(final String name, final String value, final BiPredicate predicate) {
        Gremlin.Compare compare = convertFromBiPredicate(predicate);
        Common.Value stringVal = (value == null) ? fromNull() : fromString(value);
        return hasProperty(name, compare, stringVal);
    }

    public Gremlin.FilterChain asChain(final Gremlin.FilterExp simple) {
        return Gremlin.FilterChain.newBuilder().addNode(0, Gremlin.FilterNode.newBuilder().setSingle(simple)).build();
    }
}

