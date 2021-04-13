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

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TraversalSourceGraph implements Graph {
    private GraphVariables variables = new GraphVariables();
    private Configuration configuration;
    private Features graphFeatures = new GraphFeatures();

    public static TraversalSourceGraph open(final Configuration configuration) {
        return new TraversalSourceGraph(configuration);
    }

    private TraversalSourceGraph(final Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public Features features() {
        return this.graphFeatures;
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        throw new UnsupportedOperationException("add vertex not supported");
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        throw new UnsupportedOperationException("compute not supported");
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw new UnsupportedOperationException("compute not supported");
    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        throw new UnsupportedOperationException("vertices not supported");
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        throw new UnsupportedOperationException("edges not supported");
    }

    @Override
    public Transaction tx() {
        throw new UnsupportedOperationException("tx not supported");
    }

    @Override
    public void close() throws Exception {
        // do nothing
    }

    @Override
    public Variables variables() {
        return this.variables;
    }

    @Override
    public Configuration configuration() {
        return configuration;
    }

    public class GraphVariables implements Variables {
        private Map<String, Object> variables = new ConcurrentHashMap<>();

        @Override
        public Set<String> keys() {
            return variables.keySet();
        }

        @Override
        public <R> Optional<R> get(String key) {
            return Optional.ofNullable((R) variables.get(key));
        }

        @Override
        public void set(String key, Object value) {
            variables.put(key, value);
        }

        @Override
        public void remove(String key) {
            variables.remove(key);
        }
    }

    public static final class GraphFeatures implements Features {
        private GraphFeatures graphFeatures;
        private VertexFeatures vertexFeatures;
        private EdgeFeatures edgeFeatures;
        private EdgePropertyFeatures edgePropertyFeatures;
        private VertexPropertyFeatures vertexPropertyFeatures;

        private GraphFeatures() {
            this.graphFeatures = new EmptyGraphGraphFeatures();
            this.vertexFeatures = new EmptyGraphVertexFeatures();
            this.edgeFeatures = new EmptyGraphEdgeFeatures();
            this.edgePropertyFeatures = new EmptyGraphEdgePropertyFeatures();
            this.vertexPropertyFeatures = new EmptyGraphVertexPropertyFeatures();
        }

        public GraphFeatures graph() {
            return this.graphFeatures;
        }

        public VertexFeatures vertex() {
            return this.vertexFeatures;
        }

        public EdgeFeatures edge() {
            return this.edgeFeatures;
        }

        public abstract class EmptyGraphElementFeatures implements ElementFeatures {
            public EmptyGraphElementFeatures() {
            }

            public boolean supportsAddProperty() {
                return false;
            }

            public boolean supportsRemoveProperty() {
                return false;
            }
        }

        public final class EmptyGraphEdgePropertyFeatures implements EdgePropertyFeatures {
            public EmptyGraphEdgePropertyFeatures() {
            }
        }

        public final class EmptyGraphVertexPropertyFeatures implements VertexPropertyFeatures {
            public EmptyGraphVertexPropertyFeatures() {
            }

            public boolean supportsRemoveProperty() {
                return false;
            }
        }

        public final class EmptyGraphEdgeFeatures extends EmptyGraphElementFeatures implements EdgeFeatures {
            public EmptyGraphEdgeFeatures() {
                super();
            }

            public boolean supportsAddEdges() {
                return false;
            }

            public boolean supportsRemoveEdges() {
                return false;
            }

            public EdgePropertyFeatures properties() {
                return edgePropertyFeatures;
            }
        }

        public final class EmptyGraphVertexFeatures extends EmptyGraphElementFeatures implements VertexFeatures {
            public EmptyGraphVertexFeatures() {
                super();
            }

            public VertexProperty.Cardinality getCardinality(final String key) {
                return VertexProperty.Cardinality.list;
            }

            public boolean supportsAddVertices() {
                return false;
            }

            public boolean supportsRemoveVertices() {
                return false;
            }

            public VertexPropertyFeatures properties() {
                return vertexPropertyFeatures;
            }
        }

        public final class EmptyGraphGraphFeatures implements GraphFeatures {
            public EmptyGraphGraphFeatures() {
            }

            public boolean supportsPersistence() {
                return false;
            }

            public boolean supportsTransactions() {
                return false;
            }

            public boolean supportsThreadedTransactions() {
                return false;
            }

            public VariableFeatures variables() {
                return null;
            }

            public boolean supportsComputer() {
                return false;
            }
        }
    }
}
