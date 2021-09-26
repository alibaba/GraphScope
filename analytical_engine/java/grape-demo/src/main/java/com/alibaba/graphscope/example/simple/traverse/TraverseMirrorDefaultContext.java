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

package com.alibaba.graphscope.example.simple.traverse;

import com.alibaba.fastffi.FFIByteString;
import com.alibaba.graphscope.app.DefaultContextBase;
import com.alibaba.graphscope.example.simple.sssp.mirror.SSSPEdata;
import com.alibaba.graphscope.example.simple.sssp.mirror.SSSPOid;
import com.alibaba.graphscope.example.simple.sssp.mirror.SSSPVdata;
import com.alibaba.graphscope.fragment.ImmutableEdgecutFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.stdcxx.StdVector;

public class TraverseMirrorDefaultContext
        implements DefaultContextBase<SSSPOid, Long, SSSPVdata, SSSPEdata> {

    public int step;
    public int maxStep;
    public long fake_vid;
    public double fake_edata;

    @Override
    public void Init(
            ImmutableEdgecutFragment<SSSPOid, Long, SSSPVdata, SSSPEdata> immutableEdgecutFragment,
            DefaultMessageManager javaDefaultMessageManager,
            StdVector<FFIByteString> args) {
        maxStep = Integer.parseInt(args.get(0).toString());
        step = 0;
    }

    @Override
    public void Output(
            ImmutableEdgecutFragment<SSSPOid, Long, SSSPVdata, SSSPEdata>
                    immutableEdgecutFragment) {}
}
