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
package com.github.hrpc.rpc.metrics;

import com.github.hrpc.rpc.Server;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is for maintaining  the various RPC statistics
 * and publishing them through the metrics interfaces.
 */
public class RpcMetrics {

    static final Log LOG = LogFactory.getLog(RpcMetrics.class);
    final Server server;
    final String name;

    RpcMetrics(Server server) {
        String port = String.valueOf(server.getListenerAddress().getPort());
        name = "RpcActivityForPort"+ port;
        this.server = server;
    }

    public String name() { return name; }

    public static RpcMetrics create(Server server) {
        RpcMetrics m = new RpcMetrics(server);
        return m;
    }

    AtomicLong receivedBytes = new AtomicLong();
    AtomicLong sentBytes = new AtomicLong();
    AtomicLong rpcQueueTime = new AtomicLong();
    AtomicLong rpcProcessingTime = new AtomicLong();

    public int numOpenConnections() {
        return server.getNumOpenConnections();
    }

    public int callQueueLength() {
        return server.getCallQueueLen();
    }

    /**
     * Shutdown the instrumentation for the process
     */
    //@Override
    public void shutdown() {}

    /**
     * Increment sent bytes by count
     * @param count to increment
     */
    //@Override
    public void incrSentBytes(int count) {
        sentBytes.addAndGet(count);
    }

    /**
     * Increment received bytes by count
     * @param count to increment
     */
    //@Override
    public void incrReceivedBytes(int count) {
        receivedBytes.addAndGet(count);
    }

    /**
     * Add an RPC queue time sample
     * @param qTime the queue time
     */
    //@Override
    public void addRpcQueueTime(int qTime) {
        rpcQueueTime.addAndGet(qTime);
    }

    /**
     * Add an RPC processing time sample
     * @param processingTime the processing time
     */
    //@Override
    public void addRpcProcessingTime(int processingTime) {
        rpcProcessingTime.addAndGet(processingTime);
    }
}
