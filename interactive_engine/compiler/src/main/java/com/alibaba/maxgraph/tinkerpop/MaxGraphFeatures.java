/**
 * This file is referred and derived from project hgraphdb
 *
 *   https://github.com/rayokota/hgraphdb/blob/master/src/main/java/io/hgraphdb/HBaseGraphFeatures.java
 *
 * which is licensed under Apache License 2.0.
 */
package com.alibaba.maxgraph.tinkerpop;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public class MaxGraphFeatures implements Graph.Features {
    private GraphFeatures graphFeatures = new MaxGraphFeatures.MaxGraphGraphFeatures();
    private VertexFeatures vertexFeatures = new MaxGraphFeatures.MaxGraphVertexFeatures();
    private EdgeFeatures edgeFeatures = new MaxGraphFeatures.MaxGraphEdgeFeatures();

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

    public class MaxGraphGraphFeatures implements GraphFeatures {

        private VariableFeatures variableFeatures = new MaxGraphVariables.MaxGraphVariableFeatures();

        MaxGraphGraphFeatures() {
        }

        @Override
        public boolean supportsTransactions() {
            return false;
        }

        @Override
        public boolean supportsConcurrentAccess() {
            return true;
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

    public class MaxGraphVertexFeatures extends MaxGraphFeatures.MaxGraphElementFeatures implements VertexFeatures {

        private final VertexPropertyFeatures vertexPropertyFeatures = new MaxGraphFeatures.MaxGraphVertexPropertyFeatures();

        protected MaxGraphVertexFeatures() {
        }


        @Override
        public VertexProperty.Cardinality getCardinality(final String key) {
            return VertexProperty.Cardinality.single;
        }

        @Override
        public boolean supportsAddVertices() {
            return true;
        }

        @Override
        public boolean supportsRemoveVertices() {
            return true;
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

    public class MaxGraphEdgeFeatures extends MaxGraphFeatures.MaxGraphElementFeatures implements EdgeFeatures {

        private final EdgePropertyFeatures edgePropertyFeatures = new MaxGraphFeatures.MaxGraphEdgePropertyFeatures();

        MaxGraphEdgeFeatures() {
        }


        @Override
        public boolean supportsAddEdges() {
            return true;
        }

        @Override
        public boolean supportsRemoveEdges() {
            return true;
        }

        @Override
        public EdgePropertyFeatures properties() {
            return edgePropertyFeatures;
        }
    }

    public class MaxGraphElementFeatures implements ElementFeatures {

        MaxGraphElementFeatures() {
        }

        @Override
        public boolean supportsAddProperty() {
            return true;
        }

        @Override
        public boolean supportsRemoveProperty() {
            return true;
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

    public class MaxGraphVertexPropertyFeatures extends MaxGraphFeatures.MaxGraphEdgePropertyFeatures implements VertexPropertyFeatures {

        MaxGraphVertexPropertyFeatures() {
        }

        @Override
        public boolean supportsRemoveProperty() {
            return true;
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

    public class MaxGraphEdgePropertyFeatures implements EdgePropertyFeatures {

        MaxGraphEdgePropertyFeatures() {
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
            return true;
        }

        @Override
        public boolean supportsFloatArrayValues() {
            return true;
        }

        @Override
        public boolean supportsIntegerArrayValues() {
            return true;
        }

        @Override
        public boolean supportsStringArrayValues() {
            return true;
        }

        @Override
        public boolean supportsLongArrayValues() {
            return true;
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
