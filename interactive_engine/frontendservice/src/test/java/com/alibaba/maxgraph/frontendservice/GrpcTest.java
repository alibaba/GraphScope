/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.frontendservice;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.alibaba.maxgraph.coordinator.Constants;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author xiafei.qiuxf
 * @date 2019-02-25
 */
public class GrpcTest {


    public Server getServer(int port) throws IOException {
        NettyServerBuilder serverBuilder = NettyServerBuilder
            .forAddress(new InetSocketAddress("", port))
            .maxInboundMessageSize(Constants.MAXGRAPH_RPC_MAX_MESSAGE_SIZE);

        return serverBuilder.build().start();
    }

    @Test
    public void testPortReuse() throws IOException {
        Server server1 = getServer(8987);
        try {
            Server server2 = getServer(8987);
            server2.shutdown();
        } catch (IOException e) {
            Assert.assertEquals(e.getMessage() ,"Failed to bind");
        }
        server1.shutdown();
    }
}
