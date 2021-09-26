package org.apache.giraph.graph.impl;

import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.serialization.FFIByteVectorInputStream;
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.VertexIdManager;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Default implementation for vertexId management.
 *
 * @param <OID_T> giraph vertex id type
 * @param <GRAPE_OID_T> grape vertex oid
 * @param <GRAPE_VID_T> grape vertex vid
 * @param <GRAPE_VDATA_T> grape vertex data
 * @param <GRAPE_EDATA_T> grape edge data
 */
public class VertexIdManagerImpl<
                OID_T extends WritableComparable,
                GRAPE_OID_T,
                GRAPE_VID_T,
                GRAPE_VDATA_T,
                GRAPE_EDATA_T>
        implements VertexIdManager<OID_T> {

    private static Logger logger = LoggerFactory.getLogger(VertexIdManagerImpl.class);

    private IFragment<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T> fragment;
    private long vertexNum;
    private List<OID_T> vertexIdList;
    private ImmutableClassesGiraphConfiguration<OID_T, ?, ?> conf;

    /**
     * To provide giraph users with all oids, we need to get all oids out of c++ memory, then let
     * java read the stream.
     *
     * @param fragment fragment
     * @param vertexNum number of vertices
     * @param conf configuration to use.
     */
    public VertexIdManagerImpl(
            IFragment<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T> fragment,
            long vertexNum,
            ImmutableClassesGiraphConfiguration<OID_T, ?, ?> conf) {
        this.fragment = fragment;
        this.vertexNum = vertexNum;
        this.conf = conf;
        vertexIdList = new ArrayList<OID_T>((int) vertexNum);

        FFIByteVectorInputStream inputStream = generateVertexIdStream();
        try {
            for (int i = 0; i < vertexNum; ++i) {
                WritableComparable oid = conf.createVertexId();
                oid.readFields(inputStream);
                vertexIdList.add((OID_T) oid);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        inputStream.clear();
    }

    @Override
    public OID_T getId(long lid) {
        return vertexIdList.get((int) lid);
    }

    private FFIByteVectorInputStream generateVertexIdStream() {
        FFIByteVectorOutputStream outputStream = new FFIByteVectorOutputStream();
        try {
            if (conf.getGrapeOidClass().equals(Long.class)) {
                for (Vertex<GRAPE_VID_T> vertex : fragment.vertices().locals()) {
                    Long value = (Long) fragment.getId(vertex);
                    outputStream.writeLong(value);
                }
            } else if (conf.getGrapeOidClass().equals(Integer.class)) {
                for (Vertex<GRAPE_VID_T> vertex : fragment.vertices().locals()) {
                    Integer value = (Integer) fragment.getId(vertex);
                    outputStream.writeInt(value);
                }
            } else if (conf.getGrapeOidClass().equals(Double.class)) {
                for (Vertex<GRAPE_VID_T> vertex : fragment.vertices().locals()) {
                    Double value = (Double) fragment.getId(vertex);
                    outputStream.writeDouble(value);
                }
            } else if (conf.getGrapeOidClass().equals(Float.class)) {
                for (Vertex<GRAPE_VID_T> vertex : fragment.vertices().locals()) {
                    Float value = (Float) fragment.getId(vertex);
                    outputStream.writeFloat(value);
                }
            }
            // TODO: support user defined writables.
            else {
                logger.error("Unsupported oid class: " + conf.getGrapeOidClass().getName());
            }
            outputStream.finishSetting();
            logger.info(
                    "Vertex data stream size: "
                            + outputStream.bytesWriten()
                            + ", vertices: "
                            + vertexNum);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new FFIByteVectorInputStream(outputStream.getVector());
    }
}
