package com.github.hrpc.util;

/**
 * This class contains constants for configuration keys used
 * in the common code.
 *
 */
public class CommonConfigurationKeys extends CommonConfigurationKeysPublic {

    /** How often does RPC client send pings to RPC server */
    public static final String  IPC_PING_INTERVAL_KEY = "ipc.ping.interval";
    /** Default value for IPC_PING_INTERVAL_KEY */
    public static final int     IPC_PING_INTERVAL_DEFAULT = 60000; // 1 min
    /** Enables pings from RPC client to the server */
    public static final String  IPC_CLIENT_PING_KEY = "ipc.client.ping";
    /** Default value of IPC_CLIENT_PING_KEY */
    public static final boolean IPC_CLIENT_PING_DEFAULT = true;
    /** Responses larger than this will be logged */
    public static final String  IPC_SERVER_RPC_MAX_RESPONSE_SIZE_KEY =
            "ipc.server.max.response.size";
    /** Default value for IPC_SERVER_RPC_MAX_RESPONSE_SIZE_KEY */
    public static final int     IPC_SERVER_RPC_MAX_RESPONSE_SIZE_DEFAULT =
            1024*1024;
    /** Number of threads in RPC server reading from the socket */
    public static final String  IPC_SERVER_RPC_READ_THREADS_KEY =
            "ipc.server.read.threadpool.size";
    /** Default value for IPC_SERVER_RPC_READ_THREADS_KEY */
    public static final int     IPC_SERVER_RPC_READ_THREADS_DEFAULT = 1;

    public static final String IPC_MAXIMUM_DATA_LENGTH =
            "ipc.maximum.data.length";

    public static final int IPC_MAXIMUM_DATA_LENGTH_DEFAULT = 64 * 1024 * 1024;

    /** How many calls per handler are allowed in the queue. */
    public static final String  IPC_SERVER_HANDLER_QUEUE_SIZE_KEY =
            "ipc.server.handler.queue.size";
    /** Default value for IPC_SERVER_HANDLER_QUEUE_SIZE_KEY */
    public static final int     IPC_SERVER_HANDLER_QUEUE_SIZE_DEFAULT = 100;

}
