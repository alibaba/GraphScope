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
package com.alibaba.maxgraph.tinkerpop;


import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;

/**
 * @author xiafei.qiuxf
 * @date 16/6/27
 */
public class Filter {

    public enum Compare implements IntEnum {
        /* = */
        EQUAL(0, false),
        /* != */
        NOT_EQUAL(1, false),
        /* > */
        GREATER_THAN(2, false),
        /* >= */
        GREATER_THAN_EQUAL(3, false),
        /* < */
        LESS_THAN(4, false),
        /* <= */
        LESS_THAN_EQUAL(5, false),
        /* in */
        IN(6, false),
        /* not in */
        NOT_IN(7, false),
        /* contains */
        CONTAINS(8, false),
        /* xxx and xxx */
        AND(9, true),
        /* xxx or xxx */
        OR(10, true),
        /* IS NULL */
        IS(11, false),
        /* IS NOT NULL */
        IS_NOT(12, false);

        private final int val;
        private final boolean isConnective;

        Compare(int val, boolean isConnective) {
            this.val = val;
            this.isConnective = isConnective;
        }

        @Override
        public int toInt() {
            return val;
        }
    }

    public final String property;
    public final Predicate predicate;

    public Filter(String property, Compare compare, Object value) {
        this.property = property;
        this.predicate = new Predicate(compare, value);
    }

    public Filter(String property, Predicate predicate) {
        this.property = property;
        this.predicate = predicate;
    }

    public boolean predicate(Object value) {
        return this.predicate.test(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {return true;}
        if (o == null || getClass() != o.getClass()) {return false;}
        Filter filter = (Filter)o;
        return Objects.equals(property, filter.property) &&
            Objects.equals(predicate, filter.predicate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(property, predicate);
    }

    @Override
    public String toString() {
        return Joiner.on(" ").join("[", property, predicate, "]");
    }


    public static Filter fromHasContain(final HasContainer has) {
        Predicate translate = Predicate.translate(has.getPredicate());
        return new Filter(has.getKey(), translate.compare, translate.value);
    }

    public static class Predicate {
        public final Compare compare;
        public final Object value;

        public Predicate(Compare compare, Object value) {
            this.compare = compare;
            this.value = value;
        }

        public Predicate(String compare, Object value) {
            this.compare = Compare.valueOf(compare);
            if ((this.compare == Compare.AND || this.compare == Compare.OR)
                && value instanceof Collection) {
                this.value = deserialize((Collection)value);
            } else {
                this.value = value;
            }
        }

        public List<Predicate> deserialize(final Collection value) {
            ArrayList<Predicate> predicates = new ArrayList<>(value.size());
            Collection<Map<String, Object>> v = value;
            for (Map<String, Object> stringObjectMap : v) {
                predicates.add(new Predicate((String)stringObjectMap.get("compare"), stringObjectMap.get("value")));
            }
            return predicates;
        }

        public boolean test(Object value) {
            if (value == null) {
                return false;
            }

            switch (compare) {
                case EQUAL:
                    return equals(value, this.value);
                case NOT_EQUAL:
                    return !equals(value, this.value);
                case GREATER_THAN:
                    return compare(value, this.value) > 0;
                case GREATER_THAN_EQUAL:
                    return compare(value, this.value) >= 0;
                case LESS_THAN:
                    return compare(value, this.value) < 0;
                case LESS_THAN_EQUAL:
                    return compare(value, this.value) <= 0;
                case IN:
                    return containsInCollection(value);
                case NOT_IN:
                    return !containsInCollection(value);
                case CONTAINS:
                    return containsInText(value);
                case AND:
                    return and(value);
                case OR:
                    return or(value);
                default:
                    return false;
            }
        }

        private int compare(Object v1, Object v2) {
            if (v1 instanceof String && v2 instanceof String) {
                return ((String)v1).compareTo((String)v2);
            } else if (v1 instanceof Long && v2 instanceof Number) {
                return ((Long)v1).compareTo(((Number)v2).longValue());
            } else if (v1 instanceof Integer && v2 instanceof Number) {
                return ((Integer)v1).compareTo(((Number)v2).intValue());
            } else if (v1 instanceof Double && v2 instanceof Number) {
                return ((Double)v1).compareTo(((Number)v2).doubleValue());
            } else if (v1 instanceof Float && v2 instanceof Number) {
                return ((Float)v1).compareTo(((Number)v2).floatValue());
            } else if (v1 instanceof Short && v2 instanceof Number) {
                return ((Short)v1).compareTo(((Number)v2).shortValue());
            } else {
                throw new UnsupportedOperationException("Unsupported compare on type : " + v1.getClass() + " and " + v2.getClass());
            }
        }

        private boolean equals(Object v1, Object v2) {
            if (v1 instanceof Number && v2 instanceof Number) {
                return compare(v1, v2) == 0;
            } else {
                return v1.equals(v2);
            }
        }

        private boolean containsInCollection(Object v) {
            if (this.value instanceof Collection) {
                return ((Collection)this.value).contains(v);
            } else {
                throw new UnsupportedOperationException("Unsupported 'in/not in' operator on : " + this.value);
            }
        }

        private boolean containsInText(Object v) {
            if (value instanceof String) {
                return ((String)v).contains(this.value.toString());
            } else {
                throw new UnsupportedOperationException("Unsupported 'contains' operator on : " + v);
            }
        }

        private boolean and(Object v) {
            if (this.value instanceof Collection) {
                Collection<Predicate> ps = (Collection<Predicate>)this.value;
                return ps.stream().allMatch(p -> p.test(v));
            }
            return false;
        }

        private boolean or(Object v) {
            if (this.value instanceof Collection) {
                Collection<Predicate> ps = (Collection<Predicate>)this.value;
                return ps.stream().anyMatch(p -> p.test(v));
            }
            return false;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {return true;}
            if (o == null || getClass() != o.getClass()) {return false;}
            Predicate p = (Predicate)o;
            return Objects.equals(compare, p.compare) &&
                Objects.equals(value, p.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(compare, value);
        }

        public static Predicate translate(P p) {
            if (p instanceof AndP) {
                return new Predicate(Compare.AND, translate(((AndP)p).getPredicates()));
            } else if (p instanceof OrP) {
                return new Predicate(Compare.OR, translate(((OrP)p).getPredicates()));
            }

            if (org.apache.tinkerpop.gremlin.process.traversal.Compare.eq.equals(p.getBiPredicate())) {
                return new Predicate(Compare.EQUAL, p.getValue());
            } else if (org.apache.tinkerpop.gremlin.process.traversal.Compare.neq.equals(p.getBiPredicate())) {
                return new Predicate(Compare.NOT_EQUAL, p.getValue());
            } else if (org.apache.tinkerpop.gremlin.process.traversal.Compare.lt.equals(p.getBiPredicate())) {
                return new Predicate(Compare.LESS_THAN, p.getValue());
            } else if (org.apache.tinkerpop.gremlin.process.traversal.Compare.lte.equals(p.getBiPredicate())) {
                return new Predicate(Compare.LESS_THAN_EQUAL, p.getValue());
            } else if (org.apache.tinkerpop.gremlin.process.traversal.Compare.gt.equals(p.getBiPredicate())) {
                return new Predicate(Compare.GREATER_THAN, p.getValue());
            } else if (org.apache.tinkerpop.gremlin.process.traversal.Compare.gte.equals(p.getBiPredicate())) {
                return new Predicate(Compare.GREATER_THAN_EQUAL, p.getValue());
            } else if (Contains.within.equals(p.getBiPredicate())) {
                return new Predicate(Compare.IN, p.getValue());
            } else if (Contains.without.equals(p.getBiPredicate())) {
                return new Predicate(Compare.NOT_IN, p.getValue());
            } else {
                throw new UnsupportedOperationException("" + p);
            }
        }

        @Override
        public String toString() {
            return this.compare + "(" + this.value + ")";
        }

        public static List<Predicate> translate(List<P> ps) {
            return ps.stream().map(p -> translate(p)).collect(Collectors.toList());
        }

    }
}

