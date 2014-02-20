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

/**
 * a class wraps around a server's proxy, 
 * containing a list of its supported methods.
 *
 * A list of methods with a value of null indicates that the client and server
 * have the same protocol.
 */
public class ProtocolProxy<T> {
    private Class<T> protocol;
    private T proxy;

    /**
     * Constructor
     *
     * @param protocol protocol class
     * @param proxy its proxy
     *
     */
    public ProtocolProxy(Class<T> protocol, T proxy) {
        this.protocol = protocol;
        this.proxy = proxy;
    }

    /*
     * Get the proxy
     */
    public T getProxy() {
        return proxy;
    }
}