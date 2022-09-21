/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.graphscope.runtime;

import static com.alibaba.graphscope.runtime.GraphScopeClassLoader.getTypeArgumentFromInterface;

import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.fragment.GraphXFragment;
import com.alibaba.graphscope.fragment.GraphXStringEDFragment;
import com.alibaba.graphscope.fragment.GraphXStringVDFragment;
import com.alibaba.graphscope.fragment.GraphXStringVEDFragment;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.fragment.ImmutableEdgecutFragment;
import com.alibaba.graphscope.fragment.adaptor.ArrowProjectedAdaptor;
import com.alibaba.graphscope.fragment.adaptor.GraphXFragmentAdaptor;
import com.alibaba.graphscope.fragment.adaptor.GraphXStringEDFragmentAdaptor;
import com.alibaba.graphscope.fragment.adaptor.GraphXStringVDFragmentAdaptor;
import com.alibaba.graphscope.fragment.adaptor.GraphXStringVEDFragmentAdaptor;
import com.alibaba.graphscope.fragment.adaptor.ImmutableEdgecutFragmentAdaptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Util functions to wrap immutableEdgecutFragment and ArrowProjected fragment as IFragment.
 */
public class IFragmentHelper {

    private static Logger logger = LoggerFactory.getLogger(IFragmentHelper.class);

    public static IFragment adapt2SimpleFragment(Object fragmentImpl) {
        if (fragmentImpl instanceof ArrowProjectedFragment) {
            ArrowProjectedFragment projectedFragment = (ArrowProjectedFragment) fragmentImpl;
            Class<?>[] classes =
                    getTypeArgumentFromInterface(
                            ArrowProjectedFragment.class, projectedFragment.getClass());
            if (classes.length != 4) {
                logger.error("Expected 4 actural type arguments, received: " + classes.length);
                return null;
            }
            return createArrowProjectedAdaptor(
                    classes[0], classes[1], classes[2], classes[3], projectedFragment);
        } else if (fragmentImpl instanceof ImmutableEdgecutFragment) {
            ImmutableEdgecutFragment immutableEdgecutFragment =
                    (ImmutableEdgecutFragment) fragmentImpl;
            Class<?>[] classes =
                    getTypeArgumentFromInterface(
                            ImmutableEdgecutFragment.class, immutableEdgecutFragment.getClass());
            if (classes.length != 4) {
                logger.error("Expected 4 actural type arguments, received: " + classes.length);
                return null;
            }
            return createImmutableFragmentAdaptor(
                    classes[0], classes[1], classes[2], classes[3], immutableEdgecutFragment);
        } else if (fragmentImpl instanceof GraphXFragment) {
            GraphXFragment graphXFragment = (GraphXFragment) fragmentImpl;
            Class<?>[] classes =
                    getTypeArgumentFromInterface(GraphXFragment.class, graphXFragment.getClass());
            if (classes.length != 4) {
                logger.error("Expected 4 actural type arguments, received: " + classes.length);
                return null;
            }
            return createGraphXFragmentAdaptor(
                    classes[0], classes[1], classes[2], classes[3], graphXFragment);
        } else if (fragmentImpl instanceof GraphXStringVDFragment) {
            GraphXStringVDFragment graphXStringVDFragment = (GraphXStringVDFragment) fragmentImpl;
            Class<?>[] classes =
                    getTypeArgumentFromInterface(
                            GraphXStringVDFragment.class, graphXStringVDFragment.getClass());
            if (classes.length != 4) {
                logger.error("Expected 4 actural type arguments, received: " + classes.length);
                return null;
            }
            return createGraphXStringVDFragmentAdaptor(
                    classes[0], classes[1], classes[2], classes[3], graphXStringVDFragment);
        } else if (fragmentImpl instanceof GraphXStringEDFragment) {
            GraphXStringEDFragment graphXStringEDFragment = (GraphXStringEDFragment) fragmentImpl;
            Class<?>[] classes =
                    getTypeArgumentFromInterface(
                            GraphXStringEDFragment.class, graphXStringEDFragment.getClass());
            if (classes.length != 4) {
                logger.error("Expected 4 actural type arguments, received: " + classes.length);
                return null;
            }
            return createGraphXStringEDFragmentAdaptor(
                    classes[0], classes[1], classes[2], classes[3], graphXStringEDFragment);
        } else if (fragmentImpl instanceof GraphXStringVEDFragment) {
            GraphXStringVEDFragment graphXStringVEDFragment =
                    (GraphXStringVEDFragment) fragmentImpl;
            Class<?>[] classes =
                    getTypeArgumentFromInterface(
                            GraphXStringVEDFragment.class, graphXStringVEDFragment.getClass());
            if (classes.length != 4) {
                logger.error("Expected 4 actural type arguments, received: " + classes.length);
                return null;
            }
            return createGraphXStringVEDFragmentAdaptor(
                    classes[0], classes[1], classes[2], classes[3], graphXStringVEDFragment);
        } else {
            logger.info(
                    "Provided fragment is neither a projected fragment,a immutable fragment or"
                            + " graphx fragment.");
            return null;
        }
    }

    /**
     * Create a parameterized arrowProjectedFragment Adaptor.
     *
     * @param oidClass   oidclass
     * @param vidClass   vidclass
     * @param vdataClass vertex data class
     * @param edataClass edge data class
     * @param fragment   actual fragment obj
     * @param <OID_T>    oid
     * @param <VID_T>    vid
     * @param <VDATA_T>  vdata
     * @param <EDATA_T>  edata
     * @return created adaptor.
     */
    private static <OID_T, VID_T, VDATA_T, EDATA_T>
            ArrowProjectedAdaptor<OID_T, VID_T, VDATA_T, EDATA_T> createArrowProjectedAdaptor(
                    Class<? extends OID_T> oidClass,
                    Class<? extends VID_T> vidClass,
                    Class<? extends VDATA_T> vdataClass,
                    Class<? extends EDATA_T> edataClass,
                    ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T> fragment) {
        return new ArrowProjectedAdaptor<OID_T, VID_T, VDATA_T, EDATA_T>(fragment);
    }

    /**
     * Create a parameterized immutableFragment Adaptor.
     *
     * @param oidClass   oidclass
     * @param vidClass   vidclass
     * @param vdataClass vertex data class
     * @param edataClass edge data class
     * @param fragment   actual fragment obj
     * @param <OID_T>    oid
     * @param <VID_T>    vid
     * @param <VDATA_T>  vdata
     * @param <EDATA_T>  edata
     * @return created adaptor.
     */
    private static <OID_T, VID_T, VDATA_T, EDATA_T>
            ImmutableEdgecutFragmentAdaptor<OID_T, VID_T, VDATA_T, EDATA_T>
                    createImmutableFragmentAdaptor(
                            Class<? extends OID_T> oidClass,
                            Class<? extends VID_T> vidClass,
                            Class<? extends VDATA_T> vdataClass,
                            Class<? extends EDATA_T> edataClass,
                            ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T> fragment) {
        return new ImmutableEdgecutFragmentAdaptor<>(fragment);
    }

    private static <OID_T, VID_T, VDATA_T, EDATA_T>
            GraphXFragmentAdaptor<OID_T, VID_T, VDATA_T, EDATA_T> createGraphXFragmentAdaptor(
                    Class<? extends OID_T> oidClass,
                    Class<? extends VID_T> vidClass,
                    Class<? extends VDATA_T> vdClass,
                    Class<? extends EDATA_T> edClass,
                    GraphXFragment<OID_T, VID_T, VDATA_T, EDATA_T> fragment) {
        return new GraphXFragmentAdaptor<>(fragment);
    }

    private static <OID_T, VID_T, VDATA_T, EDATA_T>
            GraphXStringVDFragmentAdaptor<OID_T, VID_T, VDATA_T, EDATA_T>
                    createGraphXStringVDFragmentAdaptor(
                            Class<? extends OID_T> oidClass,
                            Class<? extends VID_T> vidClass,
                            Class<? extends VDATA_T> vdClass,
                            Class<? extends EDATA_T> edClass,
                            GraphXStringVDFragment<OID_T, VID_T, VDATA_T, EDATA_T> fragment) {
        return new GraphXStringVDFragmentAdaptor<OID_T, VID_T, VDATA_T, EDATA_T>(fragment);
    }

    private static <OID_T, VID_T, VDATA_T, EDATA_T>
            GraphXStringEDFragmentAdaptor<OID_T, VID_T, VDATA_T, EDATA_T>
                    createGraphXStringEDFragmentAdaptor(
                            Class<? extends OID_T> oidClass,
                            Class<? extends VID_T> vidClass,
                            Class<? extends VDATA_T> vdClass,
                            Class<? extends EDATA_T> edClass,
                            GraphXStringEDFragment<OID_T, VID_T, VDATA_T, EDATA_T> fragment) {
        return new GraphXStringEDFragmentAdaptor<OID_T, VID_T, VDATA_T, EDATA_T>(fragment);
    }

    private static <OID_T, VID_T, VDATA_T, EDATA_T>
            GraphXStringVEDFragmentAdaptor<OID_T, VID_T, VDATA_T, EDATA_T>
                    createGraphXStringVEDFragmentAdaptor(
                            Class<? extends OID_T> oidClass,
                            Class<? extends VID_T> vidClass,
                            Class<? extends VDATA_T> vdClass,
                            Class<? extends EDATA_T> edClass,
                            GraphXStringVEDFragment<OID_T, VID_T, VDATA_T, EDATA_T> fragment) {
        return new GraphXStringVEDFragmentAdaptor<OID_T, VID_T, VDATA_T, EDATA_T>(fragment);
    }
}
