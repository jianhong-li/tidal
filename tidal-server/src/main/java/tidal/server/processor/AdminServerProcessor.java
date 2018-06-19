package tidal.server.processor;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

import io.netty.channel.ChannelHandlerContext;
import tidal.common.constant.LoggerName;
import tidal.common.protocol.RequestCode;
import tidal.common.protocol.ResponseCode;
import tidal.common.protocol.body.ComponentData;
import tidal.common.protocol.body.ServerStatsData;
import tidal.common.protocol.header.AdminMsgRequestHeader;
import tidal.remoting.common.RemotingHelper;
import tidal.remoting.netty.NettyRequestProcessor;
import tidal.remoting.protocol.RemotingCommand;
import tidal.server.ServerController;

public class AdminServerProcessor implements NettyRequestProcessor {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.SERVER_LOGGER_NAME);
	private final ServerController serverController;

	public AdminServerProcessor(final ServerController serverController) {
		this.serverController = serverController;
	}

	@Override
	public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
		AdminMsgRequestHeader requestHeader = //
				(AdminMsgRequestHeader) request.decodeCommandCustomHeader(AdminMsgRequestHeader.class);

		if (!this.serverController.getServerConfig().getPassword().equals(requestHeader.getPassword())) {
			RemotingCommand response = RemotingCommand.createResponseCommand(null);
			response.setCode(ResponseCode.PASSWORD_ERROR);
			response.setRemark(null);
			return response;
		}

		RemotingCommand response = null;
		switch (request.getCode()) {
		case RequestCode.SERVER_SHUTDOWN:
			response = shutdown();
			break;
		case RequestCode.CREATE_TOPIC:
			response = createTopic(request);
			break;
		case RequestCode.QUERY_ALL_TOPIC:
			response = queryAllTopic(request);
			break;
		default:
			break;
		}

		return response;
	}

	@Override
	public boolean rejectRequest() {
		return false;
	}

	private RemotingCommand shutdown() {

		System.exit(0);

		ServerStatsData serverStatsData = new ServerStatsData();
		serverStatsData.setServerInfo("The Server [" + RemotingHelper.getPhyLocalAddress() + ":"
				+ serverController.getNettyServerConfig().getListenPort() + "] is stopping!");

		RemotingCommand response = RemotingCommand.createResponseCommand(null);
		response.setCode(ResponseCode.SUCCESS);
		response.setBody(serverStatsData.encode());
		response.setRemark(null);

		return response;
	}

	private RemotingCommand createTopic(RemotingCommand request) {
		ComponentData componentData = ComponentData.decode(request.getBody(), true);

		int code = serverController.getTidalStore().putComponent(componentData.getTopic(), componentData.getPloy(),
				componentData.getInitValue(), componentData.getClassName());

		RemotingCommand response = RemotingCommand.createResponseCommand(null);
		response.setCode(code);
		response.setRemark(null);

		return response;
	}

	private RemotingCommand queryAllTopic(RemotingCommand request) {
		List<ComponentData> queryAllTopic = serverController.getTidalStore().queryAllTopic();

		RemotingCommand response = RemotingCommand.createResponseCommand(null);
		response.setCode(ResponseCode.SUCCESS);
		response.setBody(JSON.toJSONBytes(queryAllTopic, SerializerFeature.WriteNullListAsEmpty));
		response.setRemark(null);

		return response;
	}

	public String getProcessorName() {
		return AdminServerProcessor.class.getSimpleName();
	}

}
