package tidal.remoting.netty;

public class NettySystemConfig {

	public static final String TIDAL_REMOTING_NETTY_POOLED_BYTE_BUF_ALLOCATOR_ENABLE =
	        "tidal.remoting.nettyPooledByteBufAllocatorEnable";
	public static final String TIDAL_REMOTING_SOCKET_SNDBUF_SIZE = //
	        "tidal.remoting.socket.sndbuf.size";
	public static final String TIDAL_REMOTING_SOCKET_RCVBUF_SIZE = //
	        "tidal.remoting.socket.rcvbuf.size";
	public static final String TIDAL_REMOTING_CLIENT_ASYNC_SEMAPHORE_VALUE = //
			"tidal.remoting.clientAsyncSemaphoreValue";
	public static final String TIDAL_REMOTING_CLIENT_ONEWAY_SEMAPHORE_VALUE = //
			"tidal.remoting.clientOnewaySemaphoreValue";

	public static final int CLIENT_ASYNC_SEMAPHORE_VALUE = //
			Integer.parseInt(System.getProperty(TIDAL_REMOTING_CLIENT_ASYNC_SEMAPHORE_VALUE, "65535"));
	public static final int CLIENT_ONEWAY_SEMAPHORE_VALUE = //
			Integer.parseInt(System.getProperty(TIDAL_REMOTING_CLIENT_ONEWAY_SEMAPHORE_VALUE, "65535"));
	public static int socketSndbufSize = //
			Integer.parseInt(System.getProperty(TIDAL_REMOTING_SOCKET_SNDBUF_SIZE, "65535"));
	public static int socketRcvbufSize = //
			Integer.parseInt(System.getProperty(TIDAL_REMOTING_SOCKET_RCVBUF_SIZE, "65535"));
}
