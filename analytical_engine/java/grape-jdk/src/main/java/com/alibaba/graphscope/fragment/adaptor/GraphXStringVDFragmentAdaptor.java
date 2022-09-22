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

package com.alibaba.graphscope.fragment.adaptor;

import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.FragmentType;
import com.alibaba.graphscope.fragment.GraphXStringVDFragment;

public class GraphXStringVDFragmentAdaptor<OID_T, VID_T, VD_T, ED_T>
        extends AbstractGraphXFragmentAdaptor<OID_T, VID_T, VD_T, ED_T> {

    private GraphXStringVDFragment<OID_T, VID_T, VD_T, ED_T> graphXStringVDFragment;

    public GraphXStringVDFragmentAdaptor(
            GraphXStringVDFragment<OID_T, VID_T, VD_T, ED_T> graphXStringVDFragment) {
        super(graphXStringVDFragment);
        this.graphXStringVDFragment = graphXStringVDFragment;
    }

    public GraphXStringVDFragment<OID_T, VID_T, VD_T, ED_T> getFragment() {
        return graphXStringVDFragment;
    }

    /**
     * Return the underlying fragment type,i.e. ArrowProjected or Simple.
     *
     * @return underlying fragment type.
     */
    @Override
    public FragmentType fragmentType() {
        return FragmentType.GraphXStringVDFragment;
    }

    @Override
    public long getInEdgeNum() {
        return graphXStringVDFragment.getInEdgeNum();
    }

    @Override
    public long getOutEdgeNum() {
        return graphXStringVDFragment.getOutEdgeNum();
    }

    /**
     * Get the data on vertex.
     *
     * @param vertex querying vertex.
     * @return vertex data
     */
    @Override
    public VD_T getData(Vertex<VID_T> vertex) {
        throw new IllegalStateException("Not implemented");
    }
}
