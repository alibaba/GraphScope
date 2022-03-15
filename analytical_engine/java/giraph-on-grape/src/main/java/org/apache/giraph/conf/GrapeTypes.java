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
package org.apache.giraph.conf;

import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.fragment.ImmutableEdgecutFragment;
import com.alibaba.graphscope.fragment.adaptor.ArrowProjectedAdaptor;
import com.alibaba.graphscope.fragment.adaptor.ImmutableEdgecutFragmentAdaptor;

import org.apache.giraph.utils.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holding type arguments for grape fragment.
 */
public class GrapeTypes {

    private static Logger logger = LoggerFactory.getLogger(GrapeTypes.class);

    private Class<?> oidClass;
    private Class<?> vidClass;
    private Class<?> vdataClass;
    private Class<?> edataClass;

    public GrapeTypes(IFragment fragment) {
        if (!parseFromSimpleFragment(fragment)) {
            logger.error("Parse grape type from fragment failed" + fragment);
        }
    }

    public GrapeTypes(String fragStr) {
        if (!parseFromFragString(fragStr)) {
            logger.error("Parser from frag str {} failed", fragStr);
        }
    }

    public boolean hasData() {
        return oidClass != null && vidClass != null && vdataClass != null && edataClass != null;
    }

    // e.x.  gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>
    public boolean parseFromFragString(String fragStr) {
        String[] tmp = fragStr.split("<");
        if (tmp.length != 2) {
            logger.error("should be length 2 after split: " + String.join(", ", tmp));
            return false;
        }
        String[] typeParams = tmp[1].substring(0, tmp[1].length() - 1).split(",");
        if (typeParams.length != 4) {
            logger.error("should be length 4 after split: " + String.join(", ", typeParams));
            return false;
        }
        oidClass = cppType2JavaType(typeParams[0]);
        vidClass = cppType2JavaType(typeParams[1]);
        vdataClass = cppType2JavaType(typeParams[2]);
        edataClass = cppType2JavaType(typeParams[3]);
        return true;
    }

    private Class<?> cppType2JavaType(String typeString) {
        if (typeString.equals("int64_t") || typeString.equals("uint64_t")) {
            return Long.class;
        } else if (typeString.equals("int32_t") || typeString.equals("uint32_t")) {
            return Integer.class;
        } else if (typeString.equals("double")) {
            return Double.class;
        } else if (typeString.equals("float")) {
            return Float.class;
        }
        throw new IllegalStateException("Not supported type stirng" + typeString);
    }

    public boolean parseFromSimpleFragment(IFragment fragment) {

        Class<? extends IFragment> fragmentClass = fragment.getClass();
        Class<?> classes[];
        if (ImmutableEdgecutFragmentAdaptor.class.isAssignableFrom(fragmentClass)) {
            ImmutableEdgecutFragmentAdaptor adaptor = (ImmutableEdgecutFragmentAdaptor) fragment;
            classes =
                    ReflectionUtils.getTypeArgumentFromInterface(
                            ImmutableEdgecutFragment.class,
                            adaptor.getImmutableFragment().getClass());
        } else if (ArrowProjectedFragment.class.isAssignableFrom(fragmentClass)) {
            ArrowProjectedAdaptor adaptor = (ArrowProjectedAdaptor) fragment;
            classes =
                    ReflectionUtils.getTypeArgumentFromInterface(
                            ArrowProjectedFragment.class,
                            adaptor.getArrowProjectedFragment().getClass());
        } else {
            return false;
        }

        if (classes.length != 4) {
            throw new IllegalStateException(
                    "Expected 4 actual types, but received class array of length: "
                            + classes.length);
        }
        oidClass = classes[0];
        vidClass = classes[1];
        vdataClass = classes[2];
        edataClass = classes[3];
        logger.info(
                "Grape types: oid: "
                        + oidClass.getName()
                        + " vid: "
                        + vidClass.getName()
                        + " vdata: "
                        + vdataClass.getName()
                        + " edata: "
                        + edataClass.getName());
        return true;
    }

    public Class<?> getOidClass() {
        return oidClass;
    }

    public Class<?> getVidClass() {
        return vidClass;
    }

    public Class<?> getVdataClass() {
        return vdataClass;
    }

    public Class<?> getEdataClass() {
        return edataClass;
    }
}
