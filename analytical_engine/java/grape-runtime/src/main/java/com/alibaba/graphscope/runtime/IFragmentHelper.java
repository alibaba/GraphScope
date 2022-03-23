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
import com.alibaba.graphscope.fragment.ArrowProjectedStringEDFragment;
import com.alibaba.graphscope.fragment.ArrowProjectedStringVDFragment;
import com.alibaba.graphscope.fragment.ArrowProjectedStringVEDFragment;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.fragment.ImmutableEdgecutFragment;
import com.alibaba.graphscope.fragment.adaptor.ArrowProjectedAdaptor;
import com.alibaba.graphscope.fragment.adaptor.ArrowProjectedStringEDAdaptor;
import com.alibaba.graphscope.fragment.adaptor.ArrowProjectedStringVDAdaptor;
import com.alibaba.graphscope.fragment.adaptor.ArrowProjectedStringVEDAdaptor;
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
        } else if (fragmentImpl instanceof ArrowProjectedStringVDFragment) {
            ArrowProjectedStringVDFragment projectedFragment =
                    (ArrowProjectedStringVDFragment) fragmentImpl;
            Class<?>[] classes =
                    getTypeArgumentFromInterface(
                            ArrowProjectedStringVDFragment.class, projectedFragment.getClass());
            if (classes.length != 3) {
                logger.error("Expected 3 actural type arguments, received: " + classes.length);
                return null;
            }
            return createArrowProjectedStrVDAdaptor(
                    classes[0], classes[1], classes[2], projectedFragment);
        } else if (fragmentImpl instanceof ArrowProjectedStringEDFragment) {
            ArrowProjectedStringEDFragment projectedFragment =
                    (ArrowProjectedStringEDFragment) fragmentImpl;
            Class<?>[] classes =
                    getTypeArgumentFromInterface(
                            ArrowProjectedStringEDFragment.class, projectedFragment.getClass());
            if (classes.length != 3) {
                logger.error("Expected 3 actural type arguments, received: " + classes.length);
                return null;
            }
            return createArrowProjectedStrEDAdaptor(
                    classes[0], classes[1], classes[2], projectedFragment);
        } else if (fragmentImpl instanceof ArrowProjectedStringVEDFragment) {
            ArrowProjectedStringVEDFragment projectedFragment =
                    (ArrowProjectedStringVEDFragment) fragmentImpl;
            Class<?>[] classes =
                    getTypeArgumentFromInterface(
                            ArrowProjectedStringVEDFragment.class, projectedFragment.getClass());
            if (classes.length != 3) {
                logger.error("Expected 3 actural type arguments, received: " + classes.length);
                return null;
            }
            return createArrowProjectedStrVEDAdaptor(classes[0], classes[1], projectedFragment);
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
        } else {
            logger.error(
                    "Provided fragment is neither a projected fragment,a immutable fragment, {}",
                    fragmentImpl.getClass().getName());
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

    private static <OID_T, VID_T, EDATA_T>
            ArrowProjectedStringVDAdaptor<OID_T, VID_T, EDATA_T> createArrowProjectedStrVDAdaptor(
                    Class<? extends OID_T> oidClass,
                    Class<? extends VID_T> vidClass,
                    Class<? extends EDATA_T> edataClass,
                    ArrowProjectedStringVDFragment<OID_T, VID_T, EDATA_T> fragment) {
        return new ArrowProjectedStringVDAdaptor<OID_T, VID_T, EDATA_T>(fragment);
    }

    private static <OID_T, VID_T, VDATA_T>
            ArrowProjectedStringEDAdaptor<OID_T, VID_T, VDATA_T> createArrowProjectedStrEDAdaptor(
                    Class<? extends OID_T> oidClass,
                    Class<? extends VID_T> vidClass,
                    Class<? extends VDATA_T> vdataClass,
                    ArrowProjectedStringEDFragment<OID_T, VID_T, VDATA_T> fragment) {
        return new ArrowProjectedStringEDAdaptor<OID_T, VID_T, VDATA_T>(fragment);
    }

    private static <OID_T, VID_T>
            ArrowProjectedStringVEDAdaptor<OID_T, VID_T> createArrowProjectedStrVEDAdaptor(
                    Class<? extends OID_T> oidClass,
                    Class<? extends VID_T> vidClass,
                    ArrowProjectedStringVEDFragment<OID_T, VID_T> fragment) {
        return new ArrowProjectedStringVEDAdaptor<OID_T, VID_T>(fragment);
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
}
