package com.alibaba.graphscope.gaia.store;

import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.api.schema.SchemaFetcher;

public class VineyardGraphStore extends GraphStoreService {
    public static final String MODERN_PROPERTY_RESOURCE = "modern.properties.json";
    private GraphSchema graphSchema;
    private long snapshotId;

    public VineyardGraphStore(SchemaFetcher schemaFetcher) {
        super(MODERN_PROPERTY_RESOURCE);
//        this.graphSchema = schemaFetcher.getSchemaSnapshotPair().getLeft();
//        this.snapshotId = schemaFetcher.getSchemaSnapshotPair().getRight();
    }

    @Override
    public long getLabelId(String label) {
        // mock for test
        return 1000L;
        //graphSchema.getElement(label).getLabelId();
    }

    @Override
    public String getLabel(long labelId) {
        return "mock_label";
        // graphSchema.getElement((int) labelId).getLabel();
    }

    @Override
    public long getGlobalId(long labelId, long propertyId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getPropertyId(String propertyName) {
        return 1001;
        // graphSchema.getPropertyId(propertyName);
    }

    @Override
    public String getPropertyName(int propertyId) {
        return "mock_property";
        // graphSchema.getPropertyName(propertyId);
    }

    @Override
    public long getSnapShotId() {
        return 0L;
//        return this.snapshotId;
    }
}
