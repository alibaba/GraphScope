package org.apache.giraph.graph.impl;


import com.alibaba.fastffi.llvm4jni.runtime.JavaRuntime;
import com.alibaba.graphscope.ds.PropertyNbrUnit;
import com.alibaba.graphscope.ds.TypedArray;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.fragment.adaptor.ArrowProjectedAdaptor;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import java.util.Iterator;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.LongLongEdge;
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.graph.EdgeManager;
import org.apache.giraph.graph.VertexIdManager;
import org.apache.giraph.utils.LongIdParser;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PointerImmutableLongLongEdgeManager<
    GRAPE_OID_T,
    GRAPE_VID_T,
    GRAPE_VDATA_T,
    GRAPE_EDATA_T>
    implements EdgeManager<LongWritable, LongWritable> {

    private static Logger logger = LoggerFactory.getLogger(
        PointerImmutableLongLongEdgeManager.class);

    private ImmutableClassesGiraphConfiguration<? super LongWritable, ?, ? super LongWritable> conf;
    private VertexIdManager<LongWritable> vertexIdManager;
    private Vertex<GRAPE_VID_T> grapeVertex;
    private ArrowProjectedFragment<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T> fragment;
    private LongIdParser idParser;
    private PropertyNbrUnit<Long> nbrUnit;
    private long offsetBeginPtrFirstAddr;
    private long offsetEndPtrFirstAddr;
    private TypedArray<Long> edataArray;
    private long nbrUnitEleSize;
    private long nbrUnitInitAddress;

//    private LongLongEdgeIterable edgeIterable = new LongLongEdgeIterable();

    public PointerImmutableLongLongEdgeManager(
        IFragment<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T> fragment,
        VertexIdManager<? extends LongWritable> idManager,
        ImmutableClassesGiraphConfiguration<?, ?, ?> configuration) {
        idParser = new LongIdParser(fragment.fnum(), 1);
        this.fragment = ((ArrowProjectedAdaptor<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T>) fragment).getArrowProjectedFragment();
        grapeVertex =
            (Vertex<GRAPE_VID_T>)
                FFITypeFactoryhelper.newVertex(configuration.getGrapeVidClass());
        vertexIdManager = (VertexIdManager<LongWritable>) idManager;
        this.conf = (ImmutableClassesGiraphConfiguration<? super LongWritable, ?, ? super LongWritable>) configuration;

        if (!conf.getGrapeOidClass().equals(Long.class) || !conf.getGrapeEdataClass()
            .equals(Long.class)) {
            throw new IllegalStateException(
                "Inconsistency in long long edge manager " + conf.getGrapeOidClass().getSimpleName()
                    + ", " + conf.getGrapeEdataClass().getSimpleName());
        }
        //Copy all edges to java memory has too much cost, don't copy
        nbrUnit = (PropertyNbrUnit<Long>) this.fragment.getOutEdgesPtr();
        offsetEndPtrFirstAddr = this.fragment.getOEOffsetsEndPtr();
        offsetBeginPtrFirstAddr = this.fragment.getOEOffsetsBeginPtr();
        edataArray = (TypedArray<Long>) this.fragment.getEdataArrayAccessor();
        nbrUnitEleSize = nbrUnit.elementSize();
        nbrUnitInitAddress = nbrUnit.getAddress();

        logger.info("Nbrunit: [{}], offsetbeginPtr fist: [{}], end [{}], edata array [{}]", nbrUnit,
            offsetBeginPtrFirstAddr, offsetEndPtrFirstAddr, edataArray);
        logger.info("nbr unit element size: {}, init address {}", nbrUnit.elementSize(),
            nbrUnit.getAddress());
    }

    /**
     * Get the number of outgoing edges on a vertex with its lid.
     *
     * @param lid
     * @return the total number of outbound edges from this vertex
     */
    @Override
    public int getNumEdges(long lid) {
//        grapeVertex.SetValue((GRAPE_VID_T) conf.getGrapeVidClass().cast(lid));
//        return (int) (fragment.getOutgoingAdjList(grapeVertex).size());
        long offset = idParser.getOffset(lid);
        long oeBeginOffset = JavaRuntime.getLong(offsetBeginPtrFirstAddr + offset * 8);
        long oeEndOffset = JavaRuntime.getLong(offsetEndPtrFirstAddr + offset * 8);
//        logger.info("oe begin eoffset: {}, oe end offset: {}", oeBeginOffset, oeEndOffset);
        return (int) (oeEndOffset - oeBeginOffset);
    }

    /**
     * Get a read-only view of the out-edges of this vertex. Note: edge objects returned by this
     * iterable may be invalidated as soon as the next element is requested. Thus, keeping a
     * reference to an edge almost always leads to undesired behavior. Accessing the edges with
     * other methods (e.g., addEdge()) during iteration leads to undefined behavior.
     *
     * @param lid
     * @return the out edges (sort order determined by subclass implementation).
     */
    @Override
    public Iterable<Edge<LongWritable, LongWritable>> getEdges(long lid) {
        return () -> new Iterator<Edge<LongWritable, LongWritable>>() {
            long offset, oeBeginOffset, oeEndOffset, curAddress, endAddress;
            LongLongEdge edge;

            {
                offset = idParser.getOffset(lid);
                oeBeginOffset = JavaRuntime.getLong(offsetBeginPtrFirstAddr + offset * 8);
                oeEndOffset = JavaRuntime.getLong(offsetEndPtrFirstAddr + offset * 8);
                curAddress = nbrUnitInitAddress + nbrUnitEleSize * oeBeginOffset;
                endAddress = nbrUnitInitAddress + nbrUnitEleSize * oeEndOffset;
                nbrUnit.setAddress(curAddress);
//                logger.info("oe begin eoffset: {}, oe end offset: {}", oeBeginOffset, oeEndOffset);
                edge = new LongLongEdge();
            }

            @Override
            public boolean hasNext() {
                return curAddress < endAddress;
            }

            @Override
            public Edge<LongWritable, LongWritable> next() {
//                logger.info("edge of vertex {}, target id {}, edge value {}",
//                    vertexIdManager.getId(lid), vertexIdManager.getId(nbrUnit.vid()),
//                    edataArray.get(nbrUnit.eid()));
                edge.setTargetVertexId(vertexIdManager.getId(nbrUnit.vid()));
                edge.setValue(edataArray.get(nbrUnit.eid()));
                curAddress += nbrUnitEleSize;
                nbrUnit.setAddress(curAddress);
                return edge;
            }
        };
    }


    /**
     * Set the outgoing edges for this vertex.
     *
     * @param edges Iterable of edges
     */
    @Override
    public void setEdges(Iterable<Edge<LongWritable, LongWritable>> edges) {
        throw new IllegalStateException("not implemented");
    }

    /**
     * Get an iterable of out-edges that can be modified in-place. This can mean changing the
     * current edge value or removing the current edge (by using the iterator version). Note:
     * accessing the edges with other methods (e.g., addEdge()) during iteration leads to undefined
     * behavior.
     *
     * @return An iterable of mutable out-edges
     */
    @Override
    public Iterable<MutableEdge<LongWritable, LongWritable>> getMutableEdges() {
        throw new IllegalStateException("not implemented");
    }

    /**
     * Return the value of the first edge with the given target vertex id, or null if there is no
     * such edge. Note: edge value objects returned by this method may be invalidated by the next
     * call. Thus, keeping a reference to an edge value almost always leads to undesired behavior.
     *
     * @param targetVertexId Target vertex id
     * @return edge value (or null if missing)
     */
    @Override
    public LongWritable getEdgeValue(LongWritable targetVertexId) {
        throw new IllegalStateException("not implemented");
    }

    /**
     * If an edge to the target vertex exists, set it to the given edge value. This only makes sense
     * with strict graphs.
     *
     * @param targetVertexId Target vertex id
     * @param edgeValue      edge value
     */
    @Override
    public void setEdgeValue(LongWritable targetVertexId, LongWritable edgeValue) {
        throw new IllegalStateException("not implemented");
    }

    /**
     * Get an iterable over the values of all edges with the given target vertex id. This only makes
     * sense for multigraphs (i.e. graphs with parallel edges). Note: edge value objects returned by
     * this method may be invalidated as soon as the next element is requested. Thus, keeping a
     * reference to an edge value almost always leads to undesired behavior.
     *
     * @param targetVertexId Target vertex id
     * @return Iterable of edge values
     */
    @Override
    public Iterable<LongWritable> getAllEdgeValues(LongWritable targetVertexId) {
        throw new IllegalStateException("not implemented");
    }

    /**
     * Add an edge for this vertex (happens immediately)
     *
     * @param edge Edge to add
     */
    @Override
    public void addEdge(Edge<LongWritable, LongWritable> edge) {
        throw new IllegalStateException("not implemented");
    }

    /**
     * Removes all edges pointing to the given vertex id.
     *
     * @param targetVertexId the target vertex id
     */
    @Override
    public void removeEdges(LongWritable targetVertexId) {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public void unwrapMutableEdges() {
        throw new IllegalStateException("not implemented");
    }
}
