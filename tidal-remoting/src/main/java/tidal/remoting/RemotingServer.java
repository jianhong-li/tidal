package tidal.remoting;

import java.util.concurrent.ExecutorService;


import io.netty.channel.Channel;
import tidal.remoting.common.Pair;
import tidal.remoting.exception.RemotingSendRequestException;
import tidal.remoting.exception.RemotingTimeoutException;
import tidal.remoting.exception.RemotingTooMuchRequestException;
import tidal.remoting.netty.NettyRequestProcessor;
import tidal.remoting.protocol.RemotingCommand;

public interface RemotingServer extends RemotingService {

	void registerProcessor(final int requestCode, final NettyRequestProcessor processor,
			final ExecutorService executor);

	int localListenPort();
	
	Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(final int requestCode);

	RemotingCommand invokeSync(final Channel channel, final RemotingCommand request, final long timeoutMillis)
			throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException;

	void invokeAsync(final Channel channel, final RemotingCommand request, final long timeoutMillis,
			final InvokeCallback invokeCallback) throws InterruptedException, RemotingTooMuchRequestException,
			RemotingTimeoutException, RemotingSendRequestException;

	void invokeOneway(final Channel channel, final RemotingCommand request, final long timeoutMillis)
			throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException,
			RemotingSendRequestException;
}
