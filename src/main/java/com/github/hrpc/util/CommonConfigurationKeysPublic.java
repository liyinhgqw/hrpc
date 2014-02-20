package com.github.hrpc.util;

/**
 * This class contains constants for configuration keys used
 * in the common code.
 *
 * It includes all publicly documented configuration keys. In general
 * this class should not be used directly (use CommonConfigurationKeys
 * instead)
 *
 */
public class CommonConfigurationKeysPublic {

    /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
    public static final String  IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY =
            "ipc.client.connection.maxidletime";
    /** Default value for IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY */
    public static final int     IPC_CLIENT_CONNECTION_MAXIDLETIME_DEFAULT = 10000; // 10s
    /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
    public static final String  IPC_CLIENT_CONNECT_TIMEOUT_KEY =
            "ipc.client.connect.timeout";
    /** Default value for IPC_CLIENT_CONNECT_TIMEOUT_KEY */
    public static final int     IPC_CLIENT_CONNECT_TIMEOUT_DEFAULT = 20000; // 20s
    /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
    public static final String  IPC_CLIENT_CONNECT_MAX_RETRIES_KEY =
            "ipc.client.connect.max.retries";
    /** Default value for IPC_CLIENT_CONNECT_MAX_RETRIES_KEY */
    public static final int     IPC_CLIENT_CONNECT_MAX_RETRIES_DEFAULT = 10;
    /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
    public static final String  IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY =
            "ipc.client.connect.max.retries.on.timeouts";
    /** Default value for IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY */
    public static final int  IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT = 45;
    /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
    public static final String  IPC_CLIENT_TCPNODELAY_KEY =
            "ipc.client.tcpnodelay";
    /** Defalt value for IPC_CLIENT_TCPNODELAY_KEY */
    public static final boolean IPC_CLIENT_TCPNODELAY_DEFAULT = false;
    /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
    public static final String  IPC_SERVER_LISTEN_QUEUE_SIZE_KEY =
            "ipc.server.listen.queue.size";
    /** Default value for IPC_SERVER_LISTEN_QUEUE_SIZE_KEY */
    public static final int     IPC_SERVER_LISTEN_QUEUE_SIZE_DEFAULT = 128;
    /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
    public static final String  IPC_CLIENT_KILL_MAX_KEY = "ipc.client.kill.max";
    /** Default value for IPC_CLIENT_KILL_MAX_KEY */
    public static final int     IPC_CLIENT_KILL_MAX_DEFAULT = 10;
    /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
    public static final String  IPC_CLIENT_IDLETHRESHOLD_KEY =
            "ipc.client.idlethreshold";
    /** Default value for IPC_CLIENT_IDLETHRESHOLD_DEFAULT */
    public static final int     IPC_CLIENT_IDLETHRESHOLD_DEFAULT = 4000;
    /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
    public static final String  IPC_SERVER_TCPNODELAY_KEY =
            "ipc.server.tcpnodelay";
    /** Default value for IPC_SERVER_TCPNODELAY_KEY */
    public static final boolean IPC_SERVER_TCPNODELAY_DEFAULT = false;


    public static final String  IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY = "ipc.client.fallback-to-simple-auth-allowed";
    public static final boolean IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_DEFAULT = false;
}
