package com.github.hrpc.util;

import com.github.hrpc.rpc.RPC;
import com.github.hrpc.rpc.protobuf.IpcConnectionContextProtos.IpcConnectionContextProto;
import com.github.hrpc.rpc.protobuf.RpcHeaderProtos;
import com.github.hrpc.rpc.protobuf.RpcHeaderProtos.RpcKindProto;
import com.github.hrpc.rpc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import com.google.protobuf.ByteString;

import java.io.DataInput;
import java.io.IOException;

/**
 * Protobuf Utils
 */
public class ProtoUtil {

    /**
     * Read a variable length integer in the same format that ProtoBufs encodes.
     * @param in the input stream to read from
     * @return the integer
     * @throws java.io.IOException if it is malformed or EOF.
     */
    public static int readRawVarint32(DataInput in) throws IOException {
        byte tmp = in.readByte();
        if (tmp >= 0) {
            return tmp;
        }
        int result = tmp & 0x7f;
        if ((tmp = in.readByte()) >= 0) {
            result |= tmp << 7;
        } else {
            result |= (tmp & 0x7f) << 7;
            if ((tmp = in.readByte()) >= 0) {
                result |= tmp << 14;
            } else {
                result |= (tmp & 0x7f) << 14;
                if ((tmp = in.readByte()) >= 0) {
                    result |= tmp << 21;
                } else {
                    result |= (tmp & 0x7f) << 21;
                    result |= (tmp = in.readByte()) << 28;
                    if (tmp < 0) {
                        // Discard upper 32 bits.
                        for (int i = 0; i < 5; i++) {
                            if (in.readByte() >= 0) {
                                return result;
                            }
                        }
                        throw new IOException("Malformed varint");
                    }
                }
            }
        }
        return result;
    }

    static RpcHeaderProtos.RpcKindProto convert(RPC.RpcKind kind) {
        switch (kind) {
            case RPC_BUILTIN: return RpcKindProto.RPC_BUILTIN;
            case RPC_WRITABLE: return RpcKindProto.RPC_WRITABLE;
            case RPC_PROTOCOL_BUFFER: return RpcKindProto.RPC_PROTOCOL_BUFFER;
        }
        return null;
    }


    public static RPC.RpcKind convert(RpcKindProto kind) {
        switch (kind) {
            case RPC_BUILTIN: return RPC.RpcKind.RPC_BUILTIN;
            case RPC_WRITABLE: return RPC.RpcKind.RPC_WRITABLE;
            case RPC_PROTOCOL_BUFFER: return RPC.RpcKind.RPC_PROTOCOL_BUFFER;
        }
        return null;
    }

    public static RpcRequestHeaderProto makeRpcRequestHeader(RPC.RpcKind rpcKind,
                                                             RpcRequestHeaderProto.OperationProto operation, int callId,
                                                             int retryCount, byte[] uuid) {
        RpcRequestHeaderProto.Builder result = RpcRequestHeaderProto.newBuilder();
        result.setRpcKind(convert(rpcKind)).setRpcOp(operation).setCallId(callId)
                .setRetryCount(retryCount).setClientId(ByteString.copyFrom(uuid));
        return result.build();
    }

    /**
     * This method creates the connection context  using exactly the same logic
     * as the old connection context as was done for writable where
     * the effective and real users are set based on the auth method.
     *
     */
    public static IpcConnectionContextProto makeIpcConnectionContext(
            final String protocol) {
        IpcConnectionContextProto.Builder result = IpcConnectionContextProto.newBuilder();
        if (protocol != null) {
            result.setProtocol(protocol);
        }
        return result.build();
    }
}
