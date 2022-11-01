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

import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.fragment.ImmutableEdgecutFragment;
import com.alibaba.graphscope.fragment.adaptor.ArrowProjectedAdaptor;
import com.alibaba.graphscope.fragment.adaptor.ImmutableEdgecutFragmentAdaptor;
import com.alibaba.graphscope.utils.GenericUtils;

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
                    GenericUtils.getTypeArgumentFromInterface(projectedFragment.getClass());
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
                    GenericUtils.getTypeArgumentFromInterface(immutableEdgecutFragment.getClass());
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
        return new ArrowProjectedAdaptor<OID_T, VID_T, VDATA_T, EDATA_T>(
                fragment, oidClass, vidClass, vdataClass, edataClass);
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
        return new ImmutableEdgecutFragmentAdaptor<>(
                fragment, oidClass, vidClass, vdataClass, edataClass);
    }
}
