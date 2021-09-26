package com.alibaba.graphscope.factory;

import com.alibaba.graphscope.app.GiraphComputationAdaptor;
import com.alibaba.graphscope.context.GiraphComputationAdaptorContext;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.fragment.adaptor.ImmutableEdgecutFragmentAdaptor;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.EdgeManager;
import org.apache.giraph.graph.VertexDataManager;
import org.apache.giraph.graph.VertexIdManager;
import org.apache.giraph.graph.impl.EagerImmutableLongLongEdgeManager;
import org.apache.giraph.graph.impl.LazyImmutableLongLongEdgeManager;
import org.apache.giraph.graph.impl.LazyImmutableEdgeManager;
import org.apache.giraph.graph.impl.LongVidDoubleVertexDataManager;
import org.apache.giraph.graph.impl.LongVidLongVertexDataManager;
import org.apache.giraph.graph.impl.LongVidLongVertexIdManagerImpl;
import org.apache.giraph.graph.impl.PointerImmutableLongLongEdgeManager;
import org.apache.giraph.graph.impl.VertexDataManagerImpl;
import org.apache.giraph.graph.impl.VertexIdManagerImpl;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GiraphComputationFactory {

    private static Logger logger = LoggerFactory.getLogger(GiraphComputationFactory.class);

    /**
     * Create the giraph computation adaptor for the input fragment. Use fragment's actual type
     * parameters to initiate a generic adaptor.
     *
     * @param className adaptor class name
     * @param fragment  simple fragment, which is parameterized.
     * @return created adaptor.
     */
    public static <OID_T, VID_T, VDATA_T, EDATA_T>
    GiraphComputationAdaptor createGiraphComputationAdaptor(
        String className,
        ImmutableEdgecutFragmentAdaptor<OID_T, VID_T, VDATA_T, EDATA_T> fragment) {
        Class<?>[] classes = getTypeArgumentFromInterface(IFragment.class, fragment.getClass());
        if (classes.length != 4) {
            logger.error("Expected 4 type params, parsed: {}", classes.length);
            return null;
        }
        return createGiraphComputationAdaptor(classes[0], classes[1], classes[2], classes[3]);
    }

    /**
     * Create the giraph computation adaptor context for the input fragment. Use fragment's actual
     * type parameters to initiate a generic adaptor context.
     *
     * @param className adaptor class name
     * @param fragment  simple fragment, which is parameterized.
     * @return created adaptor.
     */
    public static <OID_T, VID_T, VDATA_T, EDATA_T>
    GiraphComputationAdaptorContext createGiraphComputationAdaptorContext(
        String className,
        ImmutableEdgecutFragmentAdaptor<OID_T, VID_T, VDATA_T, EDATA_T> fragment) {
        Class<?>[] classes = getTypeArgumentFromInterface(IFragment.class, fragment.getClass());
        if (classes.length != 4) {
            logger.error("Expected 4 type params, parsed: {}", classes.length);
            return null;
        }
        return createGiraphComputationAdaptorContext(
            classes[0], classes[1], classes[2], classes[3]);
    }

    public static <VDATA_T extends Writable, GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T>
    VertexDataManager<VDATA_T> createDefaultVertexDataManager(
        ImmutableClassesGiraphConfiguration conf,
        IFragment<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T> frag,
        long innerVerticesNum) {
        return createDefaultVertexDataManager(
            conf.getVertexValueClass(),
            conf.getGrapeOidClass(),
            conf.getGrapeVidClass(),
            conf.getGrapeVdataClass(),
            conf.getGrapeEdataClass(),
            frag,
            innerVerticesNum,
            conf);
    }

    private static <
        VDATA_T extends Writable,
        GRAPE_OID_T,
        GRAPE_VID_T,
        GRAPE_VDATA_T,
        GRAPE_EDATA_T>
    VertexDataManager<VDATA_T> createDefaultVertexDataManager(
        Class<? extends VDATA_T> vdataClass,
        Class<? extends GRAPE_OID_T> grapeOidClass,
        Class<? extends GRAPE_VID_T> grapeVidClass,
        Class<? extends GRAPE_VDATA_T> grapeVdataClass,
        Class<? extends GRAPE_EDATA_T> grapeEdataClass,
        IFragment fragment,
        long vertexNum,
        ImmutableClassesGiraphConfiguration conf) {
        if (conf.getGrapeVdataClass().equals(Double.class)
            && conf.getGrapeVidClass().equals(Long.class)) {
            logger.info("Creating specialized long vid double vertex data manager");
            return (VertexDataManager<VDATA_T>) new LongVidDoubleVertexDataManager<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T>(
                fragment, vertexNum, conf);
        }
        if (conf.getGrapeVidClass().equals(Long.class) && conf.getGrapeVdataClass()
            .equals(Long.class)) {
            logger.info("Creating specialized long vid long vertex data manager");
            return (VertexDataManager<VDATA_T>) new LongVidLongVertexDataManager<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T>(
                fragment, vertexNum, conf);
        }
        logger.info("Creating default VertexDataManager");
        return new VertexDataManagerImpl<
            VDATA_T, GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T>(
            fragment, vertexNum, conf);
    }

    public static <
        OID_T extends WritableComparable,
        GRAPE_OID_T,
        GRAPE_VID_T,
        GRAPE_VDATA_T,
        GRAPE_EDATA_T>
    VertexIdManager<OID_T> createDefaultVertexIdManager(
        ImmutableClassesGiraphConfiguration conf,
        IFragment<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T> frag,
        long innerVerticesNum) {
        return createDefaultVertexIdManager(
            conf.getVertexIdClass(),
            conf.getGrapeOidClass(),
            conf.getGrapeVidClass(),
            conf.getGrapeVdataClass(),
            conf.getGrapeEdataClass(),
            frag,
            innerVerticesNum,
            conf);
    }

    private static <
        OID_T extends WritableComparable,
        GRAPE_OID_T,
        GRAPE_VID_T,
        GRAPE_VDATA_T,
        GRAPE_EDATA_T>
    VertexIdManager<OID_T> createDefaultVertexIdManager(
        Class<? extends OID_T> oidClass,
        Class<? extends GRAPE_OID_T> grapeOidClass,
        Class<? extends GRAPE_VID_T> grapeVidClass,
        Class<? extends GRAPE_VDATA_T> grapeVdataClass,
        Class<? extends GRAPE_EDATA_T> grapeEdataClass,
        IFragment fragment,
        long vertexNum,
        ImmutableClassesGiraphConfiguration conf) {
        if (conf.getGrapeVidClass().equals(Long.class)
            && conf.getGrapeOidClass().equals(Long.class)) {
            logger.info("Creating specialized long vid long oid vertex id manager");
            return (VertexIdManager<OID_T>) new LongVidLongVertexIdManagerImpl<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T>(
                fragment, vertexNum, conf);
        }
        return new VertexIdManagerImpl<
            OID_T, GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T>(
            fragment, vertexNum, conf);
    }

    public static <
        OID_T extends WritableComparable,
        EDATA_T extends Writable,
        GRAPE_OID_T,
        GRAPE_VID_T,
        GRAPE_VDATA_T,
        GRAPE_EDATA_T>
    EdgeManager<OID_T, EDATA_T> createImmutableEdgeManager(
        ImmutableClassesGiraphConfiguration conf,
        IFragment<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T>
            fragment,
        VertexIdManager<OID_T> vertexIdManager) {
        return createImmutableEdgeManagerImpl(
            conf.getVertexIdClass(),
            conf.getEdgeValueClass(),
            conf.getGrapeOidClass(),
            conf.getGrapeVidClass(),
            conf.getGrapeVdataClass(),
            conf.getGrapeEdataClass(),
            fragment,
            vertexIdManager,
            conf);
    }

    private static <
        OID_T extends WritableComparable,
        EDATA_T extends Writable,
        GRAPE_OID_T,
        GRAPE_VID_T,
        GRAPE_VDATA_T,
        GRAPE_EDATA_T>
    EdgeManager<OID_T, EDATA_T>
    createImmutableEdgeManagerImpl(
        Class<? extends OID_T> oidClass,
        Class<? extends EDATA_T> edataClass,
        Class<? extends GRAPE_OID_T> grapeOidClass,
        Class<? extends GRAPE_VID_T> grapeVidClass,
        Class<? extends GRAPE_VDATA_T> grapeVdataClass,
        Class<? extends GRAPE_EDATA_T> grapeEdataClass,
        IFragment<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T>
            fragment,
        VertexIdManager<OID_T> idManager,
        ImmutableClassesGiraphConfiguration<OID_T, ?, EDATA_T> conf) {
        if (conf.getEdgeManager().equals("lazy")) {
            if (conf.getGrapeOidClass().equals(Long.class) && conf.getGrapeEdataClass()
                .equals(Long.class)) {
                logger.info("Creating lazy long long edge manager");
                return (EdgeManager<OID_T, EDATA_T>) new LazyImmutableLongLongEdgeManager<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T>(
                    fragment, (VertexIdManager<? extends LongWritable>) idManager, conf);
            }
            logger.info("creating lazy default edge manager");
            return new LazyImmutableEdgeManager<
                OID_T, EDATA_T, GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T>(
                fragment, idManager, conf);
        } else if (conf.getEdgeManager().equals("eager")) {
            if (conf.getGrapeOidClass().equals(Long.class) && conf.getGrapeEdataClass()
                .equals(Long.class)) {
                logger.info("Creating eager long long edge manager");
                return (EdgeManager<OID_T, EDATA_T>) new EagerImmutableLongLongEdgeManager<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T>(
                    fragment, (VertexIdManager<? extends LongWritable>) idManager, conf);
            } else {
                throw new IllegalStateException(
                    "For eager edge manager, oid " + conf.getGrapeOidClass().getSimpleName()
                        + ", edata " + conf.getGrapeEdataClass().getSimpleName()
                        + " not supported yet");
            }
        }
        else if (conf.getEdgeManager().equals("Pointer")){
            if (conf.getGrapeOidClass().equals(Long.class) && conf.getGrapeEdataClass()
                .equals(Long.class)) {
                logger.info("Creating Pointer long long edge manager");
                return (EdgeManager<OID_T, EDATA_T>) new PointerImmutableLongLongEdgeManager<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T>(
                    fragment, (VertexIdManager<? extends LongWritable>) idManager, conf);
            } else {
                throw new IllegalStateException(
                    "For eager edge manager, oid " + conf.getGrapeOidClass().getSimpleName()
                        + ", edata " + conf.getGrapeEdataClass().getSimpleName()
                        + " not supported yet");
            }
        }
        else {
            throw new IllegalStateException("Unrecognizable edge manager type:"+ conf.getEdgeManager());
        }
    }

    private static <OID_T, VID_T, VDATA_T, EDATA_T>
    GiraphComputationAdaptor<OID_T, VID_T, VDATA_T, EDATA_T> createGiraphComputationAdaptor(
        Class<? extends OID_T> oidClass,
        Class<? extends VID_T> vidClass,
        Class<? extends VDATA_T> vdataClass,
        Class<? extends EDATA_T> edataClass) {
        return new GiraphComputationAdaptor<OID_T, VID_T, VDATA_T, EDATA_T>();
    }

    private static <OID_T, VID_T, VDATA_T, EDATA_T>
    GiraphComputationAdaptorContext<OID_T, VID_T, VDATA_T, EDATA_T>
    createGiraphComputationAdaptorContext(
        Class<? extends OID_T> oidClass,
        Class<? extends VID_T> vidClass,
        Class<? extends VDATA_T> vdataClass,
        Class<? extends EDATA_T> edataClass) {
        return new GiraphComputationAdaptorContext<OID_T, VID_T, VDATA_T, EDATA_T>();
    }

    /**
     * Get the actual argument a child class has to implement a generic interface.
     *
     * @param baseClass  baseclass
     * @param childClass child class
     * @param <T>        type to evaluation
     * @return
     */
    private static <T> Class<?>[] getTypeArgumentFromInterface(
        Class<T> baseClass, Class<? extends T> childClass) {
        Type type = childClass.getGenericInterfaces()[0];
        Class<?>[] classes;
        if (type instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) type;
            Type[] typeParams = parameterizedType.getActualTypeArguments();
            classes = new Class<?>[typeParams.length];
            for (int i = 0; i < typeParams.length; ++i) {
                classes[i] = (Class<?>) typeParams[i];
            }
            return classes;
        } else {
            throw new IllegalStateException("Not a parameterized type");
        }
    }
}
