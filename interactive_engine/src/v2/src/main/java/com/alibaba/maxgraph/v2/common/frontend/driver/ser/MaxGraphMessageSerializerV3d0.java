package com.alibaba.maxgraph.v2.common.frontend.driver.ser;

import com.alibaba.maxgraph.v2.common.frontend.result.QueryResult;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoVersion;

/**
 * The standard Gryo serializer that uses "detached" graph elements during serialization. Detached elements such as
 * {@link QueryResult} include the label and the properties associated with it which could be more costly for
 * network serialization purposes.
 *
 * @author mengji.fy
 */
public class MaxGraphMessageSerializerV3d0 extends AbstractMaxGraphGryoMessageSerializerV3d0 {

    private static final String MIME_TYPE = MaxGraphSerTokens.MAX_GRAPH_MIME_GRYO_V3D0;
    private static final String MIME_TYPE_STRINGD = MaxGraphSerTokens.MAX_GRAPH_MIME_GRYO_V3D0 + "-stringd";

    /**
     * Creates an instance with a standard {@link GryoMapper} instance.
     */
    public MaxGraphMessageSerializerV3d0() {
        super(GryoMapper.build().version(GryoVersion.V3_0).create());
    }

    /**
     * Creates an instance with a standard {@link GryoMapper} instance. Note that the instance created by the supplied
     * builder will be overridden by {@link #configure} if it is called.
     */
    public MaxGraphMessageSerializerV3d0(final GryoMapper.Builder kryo) {
        super(kryo.version(GryoVersion.V3_0).create());
    }

    @Override
    public String[] mimeTypesSupported() {
        return new String[]{serializeToString ? MIME_TYPE_STRINGD : MIME_TYPE};
    }
}
