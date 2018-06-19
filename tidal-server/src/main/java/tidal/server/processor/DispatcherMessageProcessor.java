package tidal.server.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import tidal.common.constant.LoggerName;
import tidal.common.protocol.ResponseCode;
import tidal.common.protocol.header.SeatSendMsgBackRequestHeader;
import tidal.remoting.netty.NettyRequestProcessor;
import tidal.remoting.protocol.RemotingCommand;
import tidal.server.ServerController;

public class DispatcherMessageProcessor implements NettyRequestProcessor {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.SERVER_LOGGER_NAME);
	private final ServerController serverController;

	public DispatcherMessageProcessor(final ServerController serverController) {
		this.serverController = serverController;
	}

	@Override
	public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
		log.debug(getProcessorName() + " " + Thread.currentThread().getName());
		RemotingCommand response = RemotingCommand.createResponseCommand(SeatSendMsgBackRequestHeader.class);
		final SeatSendMsgBackRequestHeader responseHeader = (SeatSendMsgBackRequestHeader) response.readCustomHeader();

		SeatSendMsgBackRequestHeader requestHeader = (SeatSendMsgBackRequestHeader) request
				.decodeCommandCustomHeader(SeatSendMsgBackRequestHeader.class);
		String value = serverController.getTidalStore().updateComponent(requestHeader.getTopic());

		if (value == null || value == "") {
			response.setCode(ResponseCode.TOPIC_NOT_EXISTED);
			responseHeader.setTopic(requestHeader.getTopic());
		} else {
			response.setCode(ResponseCode.SUCCESS);
			responseHeader.setTopic(requestHeader.getTopic());
			responseHeader.setMessage(value);
		}
		response.setRemark(null);
		return response;
	}

	@Override
	public boolean rejectRequest() {
		// return this.serverController.getSequenceStore().isOSPageCacheBusy();
		return false;
	}

	public String getProcessorName() {
		return DispatcherMessageProcessor.class.getSimpleName();
	}
}
