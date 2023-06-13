/*
* Copyright 2020 Alibaba Group Holding Limited.

*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.alibaba.graphscope.gremlin.service;

import com.alibaba.graphscope.common.client.channel.ChannelFetcher;
import com.alibaba.graphscope.common.client.channel.HostsRpcChannelFetcher;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.FileLoadType;
import com.alibaba.graphscope.common.config.GraphConfig;
import com.alibaba.graphscope.common.manager.IrMetaQueryCallback;
import com.alibaba.graphscope.common.store.ExperimentalMetaFetcher;
import com.alibaba.graphscope.gremlin.integration.result.TestGraphFactory;

public class GraphServiceMain {
    public static final String EXPERIMENTAL = "exp";
    public static final String CSR = "csr";

    public static void main(String[] args) throws Exception {
        Configs configs = new Configs("conf/ir.compiler.properties", FileLoadType.RELATIVE_PATH);
        IrMetaQueryCallback metaQueryCallback =
                new IrMetaQueryCallback(new ExperimentalMetaFetcher(configs));
        ChannelFetcher fetcher = new HostsRpcChannelFetcher(configs);
        IrGremlinServer server = new IrGremlinServer();
        String storeType = GraphConfig.GRAPH_STORE.get(configs);
        if (storeType.equals(EXPERIMENTAL)) {
            server.start(configs, fetcher, metaQueryCallback, TestGraphFactory.EXPERIMENTAL);
        } else if (storeType.equals(CSR)) {
            server.start(configs, fetcher, metaQueryCallback, TestGraphFactory.MCSR);
        } else {
            throw new UnsupportedOperationException(
                    "store type " + storeType + " is unsupported yet");
        }
    }
}
