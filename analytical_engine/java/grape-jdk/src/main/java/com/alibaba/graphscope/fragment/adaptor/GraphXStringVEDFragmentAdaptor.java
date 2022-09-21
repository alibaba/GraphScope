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
import com.alibaba.graphscope.fragment.GraphXStringVEDFragment;

public class GraphXStringVEDFragmentAdaptor<OID_T, VID_T, VD_T, ED_T>
        extends AbstractGraphXFragmentAdaptor<OID_T, VID_T, VD_T, ED_T> {

    private GraphXStringVEDFragment<OID_T, VID_T, VD_T, ED_T> graphXStringVEDFragment;

    public GraphXStringVEDFragmentAdaptor(
            GraphXStringVEDFragment<OID_T, VID_T, VD_T, ED_T> graphXStringVEDFragment) {
        super(graphXStringVEDFragment);
        this.graphXStringVEDFragment = graphXStringVEDFragment;
    }

    public GraphXStringVEDFragment<OID_T, VID_T, VD_T, ED_T> getFragment() {
        return graphXStringVEDFragment;
    }

    /**
     * Return the underlying fragment type,i.e. ArrowProjected or Simple.
     *
     * @return underlying fragment type.
     */
    @Override
    public FragmentType fragmentType() {
        return FragmentType.GraphXStringVEDFragment;
    }

    @Override
    public long getInEdgeNum() {
        return graphXStringVEDFragment.getInEdgeNum();
    }

    @Override
    public long getOutEdgeNum() {
        return graphXStringVEDFragment.getOutEdgeNum();
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
