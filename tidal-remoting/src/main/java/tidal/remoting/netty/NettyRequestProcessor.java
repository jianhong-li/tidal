package tidal.remoting.netty;

import io.netty.channel.ChannelHandlerContext;
import tidal.remoting.protocol.RemotingCommand;

/**
 * Common remoting command processor
 */
public interface NettyRequestProcessor {
	RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception;

	boolean rejectRequest();
}