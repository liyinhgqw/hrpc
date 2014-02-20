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

import com.github.hrpc.io.Writable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.protobuf.DescriptorProtos;

public class TestRPC {

    @ProtocolInfo(protocolName = "testprotocol", protocolVersion = 1)
    public interface TestProtocol {
        void ping() throws IOException;
        void slowPing(boolean shouldSlow) throws IOException;
        void sleep(long delay) throws IOException, InterruptedException;
        String echo(String value) throws IOException;
        String[] echo(String[] value) throws IOException;
        Writable echo(Writable value) throws IOException;
        int add(int v1, int v2) throws IOException;
        int add(int[] values) throws IOException;
        int error() throws IOException;
        void testServerGet() throws IOException;
        int[] exchange(int[] values) throws IOException;

        DescriptorProtos.EnumDescriptorProto exchangeProto(
                DescriptorProtos.EnumDescriptorProto arg);
    }

    public static class TestImpl implements TestProtocol {
        int fastPingCounter = 0;

        @Override
        public void ping() {}

        @Override
        public synchronized void slowPing(boolean shouldSlow) {
            if (shouldSlow) {
                while (fastPingCounter < 2) {
                    try {
                        wait();  // slow response until two fast pings happened
                    } catch (InterruptedException ignored) {}
                }
                fastPingCounter -= 2;
            } else {
                fastPingCounter++;
                notify();
            }
        }

        @Override
        public void sleep(long delay) throws InterruptedException {
            Thread.sleep(delay);
        }

        @Override
        public String echo(String value) throws IOException { return value; }

        @Override
        public String[] echo(String[] values) throws IOException { return values; }

        @Override
        public Writable echo(Writable writable) {
            return writable;
        }
        @Override
        public int add(int v1, int v2) {
            return v1 + v2;
        }

        @Override
        public int add(int[] values) {
            int sum = 0;
            for (int i = 0; i < values.length; i++) {
                sum += values[i];
            }
            return sum;
        }

        @Override
        public int error() throws IOException {
            throw new IOException("bobo");
        }

        @Override
        public void testServerGet() throws IOException {
            if (!(Server.get() instanceof RPC.Server)) {
                throw new IOException("Server.get() failed");
            }
        }

        @Override
        public int[] exchange(int[] values) {
            for (int i = 0; i < values.length; i++) {
                values[i] = i;
            }
            return values;
        }

        @Override
        public DescriptorProtos.EnumDescriptorProto exchangeProto(DescriptorProtos.EnumDescriptorProto arg) {
            return arg;
        }
    }
}