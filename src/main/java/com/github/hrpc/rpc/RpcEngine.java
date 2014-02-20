/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.hrpc.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;
import javax.net.SocketFactory;

import com.github.hrpc.util.Option;
import com.github.hrpc.rpc.Client.ConnectionId;

/** An RPC implementation. */
public interface RpcEngine {

    /** Construct a client-side proxy object.
     * @param <T>*/
    <T> ProtocolProxy<T> getProxy(Class<T> protocol,
                                  long clientVersion, InetSocketAddress addr,
                                  Option conf,
                                  SocketFactory factory, int rpcTimeout,
                                  RetryPolicy connectionRetryPolicy) throws IOException;

    /**
     * Construct a server for a protocol implementation instance.
     *
     * @param protocol the class of protocol to use
     * @param instance the instance of protocol whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     * @param numHandlers the number of method handler threads to run
     * @param numReaders the number of reader threads to run
     * @param queueSizePerHandler the size of the queue per hander thread
     * @param verbose whether each call should be logged
     * @param portRangeConfig A config parameter that can be used to restrict
     *        the range of ports used when port is 0 (an ephemeral port)
     * @return The Server instance
     * @throws IOException on any error
     */
    RPC.Server getServer(Class<?> protocol, Object instance, String bindAddress,
                         int port, int numHandlers, int numReaders,
                         int queueSizePerHandler, boolean verbose,
                         Option conf, String portRangeConfig
    ) throws IOException;

    /**
     * Returns a proxy for ProtocolMetaInfoPB, which uses the given connection
     * id.
     * @param connId, ConnectionId to be used for the proxy.
     * @param conf, Configuration.
     * @param factory, Socket factory.
     * @return Proxy object.
     * @throws IOException
     */
    ProtocolProxy<ProtocolMetaInfoPB> getProtocolMetaInfoProxy(
            ConnectionId connId, Option conf, SocketFactory factory)
            throws IOException;
}
