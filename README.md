# HRPC

This emulates the Hadoop IPC (version 2.2.0). It has removed the features of SSL to make it feasible for intra-enterprise products.

## Installing
`mvn install` should done all the jobs. Include it in the pom.xml when using it.

## Usage 
### Defining RPC interface and implementation
#### For `Writable`
* if use `Writable` as serializable mechanism, the rpc interface should like:
```java
    @ProtocolInfo(protocolName = "testprotocol", protocolVersion = 1)
    public interface TestProtocol {
        String echo(String value) throws IOException;
        Writable echo(Writable value) throws IOException;
    }
```
and its implementation should be:
```java
    public static class TestImpl implements TestProtocol {
        @Override
        public String echo(String values) throws IOException { return values; }

        @Override
        public Writable echo(Writable writable) {
            return writable;
        }
    }
```
#### For `Protobuf`
* otherwise, if use `Protobuf` as serializable mechanism, as
```
message EchoRequestProto {
  required string message = 1;
}

message EchoResponseProto {
  required string message = 1;
}

service TestProtobufRpcProto {
  rpc echo(EchoRequestProto) returns (EchoResponseProto);
}
```
the rpc interface should simply like:
```java
    @ProtocolInfo(protocolName = "testProto", protocolVersion = 1)
    public interface TestRpcService
            extends TestProtobufRpcProto.BlockingInterface {
    }
```
and its implementation should be:
```java
    public static class PBServerImpl implements TestRpcService {
        @Override
        public EchoResponseProto echo(RpcController unused, EchoRequestProto request) 
            throws ServiceException {
            return EchoResponseProto.newBuilder().setMessage(request.getMessage()).build();
        }
    }
```
Note that the anotations of `ProtocolInfo` is required, and client ends should match the same `ProtocolInfo` when calling it.

### Server
#### For `Writable`
```java
        conf = new Option();
        // Set RPC engine to Writable RPC engine (default)
        RPC.setProtocolEngine(conf, TestProtocol.class, WritableRpcEngine.class);
        // Get RPC server for server side implementation
        server = new RPC.Builder(conf).setProtocol(TestProtocol.class)
                .setInstance(new TestImpl()).setBindAddress(ADDRESS)
                .setPort(PORT).setNumHandlers(serverThreadsCount).build();
        // Add more services
        server.addProtocol(RPC.RpcKind.RPC_WRITABLE, TestRpcService2.class,service2);
        // Start
        server.start();
```

#### For `Protobuf`
```java
        conf = new Option();
        // Set RPC engine to protobuf RPC engine
        RPC.setProtocolEngine(conf, TestRpcService.class, ProtobufRpcEngine.class);
        // Create server side implementation
        PBServerImpl serverImpl = new PBServerImpl();
        BlockingService service = TestProtobufRpcProto.newReflectiveBlockingService(serverImpl);
        // Get RPC server for server side implementation
        server = new RPC.Builder(conf).setProtocol(TestRpcService.class)
                .setInstance(service).setBindAddress(ADDRESS).setPort(PORT).build();
        // Add more services
        server.addProtocol(RPC.RpcKind.RPC_PROTOCOL_BUFFER, TestRpcService2.class,service2);
        // Start
        server.start();
```


### Client
#### For `Writable`
```java
        conf = new Option();
        // Set RPC engine to protobuf RPC engine
        RPC.setProtocolEngine(conf, TestProtocol.class, WritableRpcEngine.class);
        // Get proxy, 0 is clientVersion, currently unused and reserved
        TestRpcService client = RPC.getProxy(TestProtocol.class, 0, addr, conf);
        // Test echo method
        client.echo(msg);
```
#### For `Protobuf`
```java
        conf = new Option();
        // Set RPC engine to protobuf RPC engine
        RPC.setProtocolEngine(conf, TestRpcService.class,ProtobufRpcEngine.class);
        // Get proxy, 0 is clientVersion, currently unused and reserved
        TestRpcService client = RPC.getProxy(TestRpcService.class, 0, addr, conf);
        // Test echo method
        EchoRequestProto echoRequest = EchoRequestProto.newBuilder().setMessage("hello").build();
        EchoResponseProto echoResponse = client.echo(null, echoRequest);
```
## Features
* It supports Protubuf and Writable RPC engines.
* It supports many bultin types: `Boolean`,`Character`,`Byte`,`Short`,`Integer`,`Long`,`Float`,`Double`,`Void`,`String`, and corresponding arrays.
* User can set network options like `IPC_SERVER_HANDLER_QUEUE_SIZE` and `IPC_SERVER_RPC_READ_THREADS`, etc.

