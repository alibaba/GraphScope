/**
 * This file is referred and derived from project hgraphdb
 *
 *   https://github.com/rayokota/hgraphdb/blob/master/src/main/java/io/hgraphdb/HBaseGraphFeatures.java
 *
 * which is licensed under Apache License 2.0.
 */
package com.alibaba.maxgraph.tinkerpop;

import org.apache.tinkerpop.gremlin.structure.Graph.Features;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public class BigGraphFeatures implements Features {

    protected GraphFeatures graphFeatures = new BigGraphGraphFeatures();
    protected VertexFeatures vertexFeatures = new BigGraphVertexFeatures();
    protected EdgeFeatures edgeFeatures = new BigGraphEdgeFeatures();

    @Override
    public GraphFeatures graph() {
        return graphFeatures;
    }

    @Override
    public VertexFeatures vertex() {
        return vertexFeatures;
    }

    @Override
    public EdgeFeatures edge() {
        return edgeFeatures;
    }

    @Override
    public String toString() {
        return StringFactory.featureString(this);
    }

    public class BigGraphGraphFeatures implements GraphFeatures {

        private VariableFeatures variableFeatures = new BigGraphVariables.BigGraphVariableFeatures();

        BigGraphGraphFeatures() {
        }

        @Override
        public boolean supportsTransactions() {
            return false;
        }

        @Override
        public boolean supportsConcurrentAccess() {
            return false;
        }

        @Override
        public boolean supportsComputer() {
            return false;
        }

        @Override
        public VariableFeatures variables() {
            return variableFeatures;
        }

        @Override
        public boolean supportsThreadedTransactions() {
            return false;
        }
    }

    public class BigGraphVertexFeatures extends BigGraphElementFeatures implements VertexFeatures {

        private final VertexPropertyFeatures vertexPropertyFeatures = new BigGraphVertexPropertyFeatures();

        protected BigGraphVertexFeatures() {
        }


        @Override
        public VertexProperty.Cardinality getCardinality(final String key) {
            return VertexProperty.Cardinality.single;
        }

        @Override
        public boolean supportsAddVertices() {
            return false;
        }

        @Override
        public boolean supportsRemoveVertices() {
            return false;
        }

        @Override
        public boolean supportsMultiProperties() {
            return false;
        }

        @Override
        public boolean supportsMetaProperties() {
            return false;
        }

        @Override
        public VertexPropertyFeatures properties() {
            return vertexPropertyFeatures;
        }

    }

    public class BigGraphEdgeFeatures extends BigGraphElementFeatures implements EdgeFeatures {

        private final EdgePropertyFeatures edgePropertyFeatures = new BigGraphEdgePropertyFeatures();

        BigGraphEdgeFeatures() {
        }


        @Override
        public boolean supportsAddEdges() {
            return false;
        }

        @Override
        public boolean supportsRemoveEdges() {
            return false;
        }

        @Override
        public EdgePropertyFeatures properties() {
            return edgePropertyFeatures;
        }
    }

    public class BigGraphElementFeatures implements ElementFeatures {

        BigGraphElementFeatures() {
        }

        @Override
        public boolean supportsAddProperty() {
            return false;
        }

        @Override
        public boolean supportsRemoveProperty() {
            return false;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return false;
        }

        @Override
        public boolean supportsNumericIds() {
            return false;
        }

        @Override
        public boolean supportsStringIds() {
            return false;
        }

        @Override
        public boolean supportsUuidIds() {
            return false;
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean willAllowId(final Object id) {
            return true;
        }
    }

    public class BigGraphVertexPropertyFeatures extends BigGraphEdgePropertyFeatures implements VertexPropertyFeatures {

        BigGraphVertexPropertyFeatures() {
        }

        @Override
        public boolean supportsRemoveProperty() {
            return false;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return false;
        }

        @Override
        public boolean supportsNumericIds() {
            return false;
        }

        @Override
        public boolean supportsStringIds() {
            return false;
        }

        @Override
        public boolean supportsUuidIds() {
            return false;
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean supportsMapValues() {
            return false;
        }

        @Override
        public boolean supportsMixedListValues() {
            return false;
        }

        @Override
        public boolean supportsSerializableValues() {
            return false;
        }

        @Override
        public boolean supportsUniformListValues() {
            return false;
        }

    }

    public class BigGraphEdgePropertyFeatures implements EdgePropertyFeatures {

        BigGraphEdgePropertyFeatures() {
        }

        @Override
        public boolean supportsBooleanValues() {
            return true;
        }

        @Override
        public boolean supportsByteValues() {
            return true;
        }

        @Override
        public boolean supportsDoubleValues() {
            return true;
        }

        @Override
        public boolean supportsFloatValues() {
            return true;
        }

        @Override
        public boolean supportsIntegerValues() {
            return true;
        }

        @Override
        public boolean supportsLongValues() {
            return true;
        }

        @Override
        public boolean supportsMapValues() {
            return false;
        }

        @Override
        public boolean supportsMixedListValues() {
            return false;
        }

        @Override
        public boolean supportsBooleanArrayValues() {
            return false;
        }

        @Override
        public boolean supportsByteArrayValues() {
            return true;
        }

        @Override
        public boolean supportsDoubleArrayValues() {
            return false;
        }

        @Override
        public boolean supportsFloatArrayValues() {
            return false;
        }

        @Override
        public boolean supportsIntegerArrayValues() {
            return false;
        }

        @Override
        public boolean supportsStringArrayValues() {
            return false;
        }

        @Override
        public boolean supportsLongArrayValues() {
            return false;
        }

        @Override
        public boolean supportsSerializableValues() {
            return false;
        }

        @Override
        public boolean supportsStringValues() {
            return true;
        }

        @Override
        public boolean supportsUniformListValues() {
            return false;
        }
    }
}
