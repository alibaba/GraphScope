/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.factory;

import com.alibaba.graphscope.app.GiraphComputationAdaptor;
import com.alibaba.graphscope.context.GiraphComputationAdaptorContext;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.fragment.adaptor.ImmutableEdgecutFragmentAdaptor;
import com.alibaba.graphscope.graph.GiraphEdgeManager;
import com.alibaba.graphscope.graph.GiraphVertexIdManager;
import com.alibaba.graphscope.graph.VertexDataManager;
import com.alibaba.graphscope.graph.impl.DefaultImmutableEdgeManager;
import com.alibaba.graphscope.graph.impl.GiraphVertexIdManagerImpl;
import com.alibaba.graphscope.graph.impl.VertexDataManagerImpl;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

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
        return createGiraphComputationAdaptorImpl(classes[0], classes[1], classes[2], classes[3]);
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
        return createGiraphComputationAdaptorContextImpl(
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
            GiraphVertexIdManager<GRAPE_VID_T, OID_T> createDefaultVertexIdManager(
                    ImmutableClassesGiraphConfiguration conf,
                    IFragment<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T> frag,
                    long fragVerticesNum) {
        return createDefaultVertexIdManager(
                conf.getVertexIdClass(),
                conf.getGrapeOidClass(),
                conf.getGrapeVidClass(),
                conf.getGrapeVdataClass(),
                conf.getGrapeEdataClass(),
                frag,
                fragVerticesNum,
                conf);
    }

    private static <
                    OID_T extends WritableComparable,
                    GRAPE_OID_T,
                    GRAPE_VID_T,
                    GRAPE_VDATA_T,
                    GRAPE_EDATA_T>
            GiraphVertexIdManager<GRAPE_VID_T, OID_T> createDefaultVertexIdManager(
                    Class<? extends OID_T> oidClass,
                    Class<? extends GRAPE_OID_T> grapeOidClass,
                    Class<? extends GRAPE_VID_T> grapeVidClass,
                    Class<? extends GRAPE_VDATA_T> grapeVdataClass,
                    Class<? extends GRAPE_EDATA_T> grapeEdataClass,
                    IFragment fragment,
                    long vertexNum,
                    ImmutableClassesGiraphConfiguration conf) {
        return new GiraphVertexIdManagerImpl<
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
            GiraphEdgeManager<OID_T, EDATA_T> createImmutableEdgeManager(
                    ImmutableClassesGiraphConfiguration conf,
                    IFragment<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T> fragment,
                    GiraphVertexIdManager<GRAPE_VID_T, OID_T> vertexIdManager) {
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
            GiraphEdgeManager<OID_T, EDATA_T> createImmutableEdgeManagerImpl(
                    Class<? extends OID_T> oidClass,
                    Class<? extends EDATA_T> edataClass,
                    Class<? extends GRAPE_OID_T> grapeOidClass,
                    Class<? extends GRAPE_VID_T> grapeVidClass,
                    Class<? extends GRAPE_VDATA_T> grapeVdataClass,
                    Class<? extends GRAPE_EDATA_T> grapeEdataClass,
                    IFragment<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T> fragment,
                    GiraphVertexIdManager<GRAPE_VID_T, OID_T> idManager,
                    ImmutableClassesGiraphConfiguration<OID_T, ?, EDATA_T> conf) {
        if (conf.getEdgeManager().equals("default")) {
            logger.info("Using [Default] edge manger");
            return new DefaultImmutableEdgeManager<
                    GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T, OID_T, EDATA_T>(
                    fragment, idManager, conf);
        }
        throw new IllegalStateException(
                "Unrecognizable edge manager type:" + conf.getEdgeManager());
    }

    private static <OID_T, VID_T, VDATA_T, EDATA_T>
            GiraphComputationAdaptor<OID_T, VID_T, VDATA_T, EDATA_T>
                    createGiraphComputationAdaptorImpl(
                            Class<? extends OID_T> oidClass,
                            Class<? extends VID_T> vidClass,
                            Class<? extends VDATA_T> vdataClass,
                            Class<? extends EDATA_T> edataClass) {
        return new GiraphComputationAdaptor<OID_T, VID_T, VDATA_T, EDATA_T>();
    }

    private static <OID_T, VID_T, VDATA_T, EDATA_T>
            GiraphComputationAdaptorContext<OID_T, VID_T, VDATA_T, EDATA_T>
                    createGiraphComputationAdaptorContextImpl(
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
