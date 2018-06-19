package tidal.server.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import tidal.common.constant.LoggerName;
import tidal.common.protocol.RequestCode;
import tidal.common.protocol.ResponseCode;
import tidal.common.protocol.heartbeat.HeartbeatData;
import tidal.remoting.netty.NettyRequestProcessor;
import tidal.remoting.protocol.RemotingCommand;
import tidal.server.ServerController;

public class ClientManageProcessor implements NettyRequestProcessor {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.SERVER_LOGGER_NAME);
	private final ServerController serverController;

	public ClientManageProcessor(final ServerController serverController) {
		this.serverController = serverController;
	}

	@Override
	public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
		switch (request.getCode()) {
		case RequestCode.HEART_BEAT:
			return this.heartBeat(ctx, request);
		default:
			break;
		}
		return null;
	}

	public RemotingCommand heartBeat(ChannelHandlerContext ctx, RemotingCommand request) {
		RemotingCommand response = RemotingCommand.createResponseCommand(null);

		HeartbeatData heartbeatData = HeartbeatData.decode(request.getBody(), HeartbeatData.class);
		log.debug("receive " + heartbeatData.getClientID() + " heartbeat.");

		response.setCode(ResponseCode.SUCCESS);
		response.setRemark(null);
		return response;
	}

	@Override
	public boolean rejectRequest() {
		return false;
	}

}
