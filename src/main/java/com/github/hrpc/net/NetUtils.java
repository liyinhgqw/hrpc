package com.github.hrpc.net;

import com.github.hrpc.rpc.RPC;
import com.github.hrpc.rpc.Server;
import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.net.SocketFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.net.*;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;

/**
 * Net utils.
 */
public class NetUtils {
    private static final Log LOG = LogFactory.getLog(NetUtils.class);

    /** text included in wrapped exceptions if the host is null: {@value} */
    public static final String UNKNOWN_HOST = "(unknown)";

    private static Map<String, String> hostToResolved =
            new HashMap<String, String>();

    /**
     * Take an IOException , the local host port and remote host port details and
     * return an IOException with the input exception as the cause and also
     * include the host details. The new exception provides the stack trace of the
     * place where the exception is thrown and some extra diagnostics information.
     * If the exception is BindException or ConnectException or
     * UnknownHostException or SocketTimeoutException, return a new one of the
     * same type; Otherwise return an IOException.
     *
     * @param destHost target host (nullable)
     * @param destPort target port
     * @param localHost local host (nullable)
     * @param localPort local port
     * @param exception the caught exception.
     * @return an exception to throw
     */
    public static IOException wrapException(final String destHost,
                                            final int destPort,
                                            final String localHost,
                                            final int localPort,
                                            final IOException exception) {
        if (exception instanceof BindException) {
            return new BindException(
                    "Problem binding to ["
                            + localHost
                            + ":"
                            + localPort
                            + "] "
                            + exception
                            + ";"
                            + see("BindException"));
        } else if (exception instanceof ConnectException) {
            // connection refused; include the host:port in the error
            return wrapWithMessage(exception,
                    "Call From "
                            + localHost
                            + " to "
                            + destHost
                            + ":"
                            + destPort
                            + " failed on connection exception: "
                            + exception
                            + ";"
                            + see("ConnectionRefused"));
        } else if (exception instanceof UnknownHostException) {
            return wrapWithMessage(exception,
                    "Invalid host name: "
                            + getHostDetailsAsString(destHost, destPort, localHost)
                            + exception
                            + ";"
                            + see("UnknownHost"));
        } else if (exception instanceof SocketTimeoutException) {
            return wrapWithMessage(exception,
                    "Call From "
                            + localHost + " to " + destHost + ":" + destPort
                            + " failed on socket timeout exception: " + exception
                            + ";"
                            + see("SocketTimeout"));
        } else if (exception instanceof NoRouteToHostException) {
            return wrapWithMessage(exception,
                    "No Route to Host from  "
                            + localHost + " to " + destHost + ":" + destPort
                            + " failed on socket timeout exception: " + exception
                            + ";"
                            + see("NoRouteToHost"));
        }
        else {
            return (IOException) new IOException("Failed on local exception: "
                    + exception
                    + "; Host Details : "
                    + getHostDetailsAsString(destHost, destPort, localHost))
                    .initCause(exception);

        }
    }

    /**
     * For here just return empty string
     *
     * @param entry Exception name
     * @return
     */
    private static String see(final String entry) {
        return "";
    }

    @SuppressWarnings("unchecked")
    private static <T extends IOException> T wrapWithMessage(
            T exception, String msg) {
        Class<? extends Throwable> clazz = exception.getClass();
        try {
            Constructor<? extends Throwable> ctor = clazz.getConstructor(String.class);
            Throwable t = ctor.newInstance(msg);
            return (T)(t.initCause(exception));
        } catch (Throwable e) {
            LOG.warn("Unable to wrap exception of type " +
                    clazz + ": it has no (String) constructor", e);
            return exception;
        }
    }

    /**
     * Get the host details as a string
     * @param destHost destinatioon host (nullable)
     * @param destPort destination port
     * @param localHost local host (nullable)
     * @return a string describing the destination host:port and the local host
     */
    private static String getHostDetailsAsString(final String destHost,
                                                 final int destPort,
                                                 final String localHost) {
        StringBuilder hostDetails = new StringBuilder(27);
        hostDetails.append("local host is: ")
                .append(quoteHost(localHost))
                .append("; ");
        hostDetails.append("destination host is: ").append(quoteHost(destHost))
                .append(":")
                .append(destPort).append("; ");
        return hostDetails.toString();
    }

    /**
     * Quote a hostname if it is not null
     * @param hostname the hostname; nullable
     * @return a quoted hostname or {@link #UNKNOWN_HOST} if the hostname is null
     */
    private static String quoteHost(final String hostname) {
        return (hostname != null) ?
                ("\"" + hostname + "\"")
                : UNKNOWN_HOST;
    }

    /**
     * Get the default socket factory as specified by the configuration
     * parameter <tt>hadoop.rpc.socket.factory.default</tt>
     *
     * @return the default socket factory as specified in the configuration or
     *         the JVM default socket factory if the configuration does not
     *         contain a default socket factory property.
     */
    public static SocketFactory getDefaultSocketFactory() {

//        return SocketFactory.getDefault();
        return new StandardSocketFactory();
    }


    /**
     * Same as <code>getInputStream(socket, socket.getSoTimeout()).</code>
     * <br><br>
     *
     * @see #getInputStream(Socket, long)
     */
    public static SocketInputWrapper getInputStream(Socket socket)
            throws IOException {
        return getInputStream(socket, socket.getSoTimeout());
    }

    /**
     * Return a {@link SocketInputWrapper} for the socket and set the given
     * timeout. If the socket does not have an associated channel, then its socket
     * timeout will be set to the specified value. Otherwise, a
     * {@link SocketInputStream} will be created which reads with the configured
     * timeout.
     *
     * Any socket created using socket factories returned by {@link #NetUtils},
     * must use this interface instead of {@link Socket#getInputStream()}.
     *
     * In general, this should be called only once on each socket: see the note
     * in {@link SocketInputWrapper#setTimeout(long)} for more information.
     *
     * @see Socket#getChannel()
     *
     * @param socket
     * @param timeout timeout in milliseconds. zero for waiting as
     *                long as necessary.
     * @return SocketInputWrapper for reading from the socket.
     * @throws IOException
     */
    public static SocketInputWrapper getInputStream(Socket socket, long timeout)
            throws IOException {
        InputStream stm = (socket.getChannel() == null) ?
                socket.getInputStream() : new SocketInputStream(socket);
        SocketInputWrapper w = new SocketInputWrapper(socket, stm);
        w.setTimeout(timeout);
        return w;
    }

    /**
     * Same as getOutputStream(socket, 0). Timeout of zero implies write will
     * wait until data is available.<br><br>
     *
     * From documentation for {@link #getOutputStream(Socket, long)} : <br>
     * Returns OutputStream for the socket. If the socket has an associated
     * SocketChannel then it returns a
     * {@link SocketOutputStream} with the given timeout. If the socket does not
     * have a channel, {@link Socket#getOutputStream()} is returned. In the later
     * case, the timeout argument is ignored and the write will wait until
     * data is available.<br><br>
     *
     * Any socket created using socket factories returned by {@link NetUtils},
     * must use this interface instead of {@link Socket#getOutputStream()}.
     *
     * @see #getOutputStream(Socket, long)
     *
     * @param socket
     * @return OutputStream for writing to the socket.
     * @throws IOException
     */
    public static OutputStream getOutputStream(Socket socket)
            throws IOException {
        return getOutputStream(socket, 0);
    }

    /**
     * Returns OutputStream for the socket. If the socket has an associated
     * SocketChannel then it returns a
     * {@link SocketOutputStream} with the given timeout. If the socket does not
     * have a channel, {@link Socket#getOutputStream()} is returned. In the later
     * case, the timeout argument is ignored and the write will wait until
     * data is available.<br><br>
     *
     * Any socket created using socket factories returned by {@link NetUtils},
     * must use this interface instead of {@link Socket#getOutputStream()}.
     *
     * @see Socket#getChannel()
     *
     * @param socket
     * @param timeout timeout in milliseconds. This may not always apply. zero
     *        for waiting as long as necessary.
     * @return OutputStream for writing to the socket.
     * @throws IOException
     */
    public static OutputStream getOutputStream(Socket socket, long timeout)
            throws IOException {
        return (socket.getChannel() == null) ?
                socket.getOutputStream() : new SocketOutputStream(socket, timeout);
    }

    /**
     * This is a drop-in replacement for
     * {@link Socket#connect(SocketAddress, int)}.
     * In the case of normal sockets that don't have associated channels, this
     * just invokes <code>socket.connect(endpoint, timeout)</code>. If
     * <code>socket.getChannel()</code> returns a non-null channel,
     * connect is implemented using Hadoop's selectors. This is done mainly
     * to avoid Sun's connect implementation from creating thread-local
     * selectors, since Hadoop does not have control on when these are closed
     * and could end up taking all the available file descriptors.
     *
     * @see java.net.Socket#connect(java.net.SocketAddress, int)
     *
     * @param socket
     * @param address the remote address
     * @param timeout timeout in milliseconds
     */
    public static void connect(Socket socket,
                               SocketAddress address,
                               int timeout) throws IOException {
        connect(socket, address, null, timeout);
    }

    /**
     * Like {@link NetUtils#connect(Socket, SocketAddress, int)} but
     * also takes a local address and port to bind the socket to.
     *
     * @param socket
     * @param endpoint the remote address
     * @param localAddr the local address to bind the socket to
     * @param timeout timeout in milliseconds
     */
    public static void connect(Socket socket,
                               SocketAddress endpoint,
                               SocketAddress localAddr,
                               int timeout) throws IOException {
        if (socket == null || endpoint == null || timeout < 0) {
            throw new IllegalArgumentException("Illegal argument for connect()");
        }

        SocketChannel ch = socket.getChannel();

        if (localAddr != null) {
            Class localClass = localAddr.getClass();
            Class remoteClass = endpoint.getClass();
            Preconditions.checkArgument(localClass.equals(remoteClass),
                    "Local address %s must be of same family as remote address %s.",
                    localAddr, endpoint);
            socket.bind(localAddr);
        }

        try {
            if (ch == null) {
                // let the default implementation handle it.
                socket.connect(endpoint, timeout);
            } else {
                SocketIOWithTimeout.connect(ch, endpoint, timeout);
            }
        } catch (SocketTimeoutException ste) {
            throw new ConnectTimeoutException(ste.getMessage());
        }

        // There is a very rare case allowed by the TCP specification, such that
        // if we are trying to connect to an endpoint on the local machine,
        // and we end up choosing an ephemeral port equal to the destination port,
        // we will actually end up getting connected to ourself (ie any data we
        // send just comes right back). This is only possible if the target
        // daemon is down, so we'll treat it like connection refused.
        if (socket.getLocalPort() == socket.getPort() &&
                socket.getLocalAddress().equals(socket.getInetAddress())) {
            LOG.info("Detected a loopback TCP socket, disconnecting it");
            socket.close();
            throw new ConnectException(
                    "Localhost targeted connection resulted in a loopback. " +
                            "No daemon is listening on the target port.");
        }
    }

    /**
     * Return hostname without throwing exception.
     * @return hostname
     */
    public static String getHostname() {
        try {return "" + InetAddress.getLocalHost();}
        catch(UnknownHostException uhe) {return "" + uhe;}
    }

    /**
     * Returns InetSocketAddress that a client can use to
     * connect to the server. Server.getListenerAddress() is not correct when
     * the server binds to "0.0.0.0". This returns "hostname:port" of the server,
     * or "127.0.0.1:port" when the getListenerAddress() returns "0.0.0.0:port".
     *
     * @param server
     * @return socket address that a client can use to connect to the server.
     */
    public static InetSocketAddress getConnectAddress(Server server) {
        return getConnectAddress(server.getListenerAddress());
    }

    /**
     * Returns an InetSocketAddress that a client can use to connect to the
     * given listening address.
     *
     * @param addr of a listener
     * @return socket address that a client can use to connect to the server.
     */
    public static InetSocketAddress getConnectAddress(InetSocketAddress addr) {
        if (!addr.isUnresolved() && addr.getAddress().isAnyLocalAddress()) {
            try {
                addr = new InetSocketAddress(InetAddress.getLocalHost(), addr.getPort());
            } catch (UnknownHostException uhe) {
                // shouldn't get here unless the host doesn't have a loopback iface
                addr = new InetSocketAddress("127.0.0.1", addr.getPort());
            }
        }
        return addr;
    }

    /**
     * Return a free port number. There is no guarantee it will remain free, so
     * it should be used immediately.
     *
     * @returns A free port for binding a local socket
     */
    public static int getFreeSocketPort() {
        int port = 0;
        try {
            ServerSocket s = new ServerSocket(0);
            port = s.getLocalPort();
            s.close();
            return port;
        } catch (IOException e) {
            // Could not get a free port. Return default port 0.
        }
        return port;
    }

    public static InetSocketAddress createSocketAddr(String host, int port) {
        return new InetSocketAddress(host, port);
    }
}
