package tidal.client.impl;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import tidal.client.AdminCallback;
import tidal.client.AdminResult;
import tidal.client.SendCallback;
import tidal.client.SendResult;
import tidal.client.SendStatus;
import tidal.client.exception.TidalServerException;
import tidal.common.protocol.RequestCode;
import tidal.common.protocol.ResponseCode;
import tidal.common.protocol.body.ServerStatsData;
import tidal.common.protocol.header.SeatSendMsgBackRequestHeader;
import tidal.common.protocol.heartbeat.HeartbeatData;
import tidal.remoting.CommandCustomHeader;
import tidal.remoting.InvokeCallback;
import tidal.remoting.RemotingClient;
import tidal.remoting.exception.RemotingCommandException;
import tidal.remoting.exception.RemotingConnectException;
import tidal.remoting.exception.RemotingException;
import tidal.remoting.exception.RemotingSendRequestException;
import tidal.remoting.exception.RemotingTimeoutException;
import tidal.remoting.netty.NettyClientConfig;
import tidal.remoting.netty.NettyRemotingClient;
import tidal.remoting.netty.ResponseFuture;
import tidal.remoting.protocol.RemotingCommand;

public class TidalClientAPIImpl {

	private final static Charset CHARSET_UTF8 = Charset.forName("UTF-8");

	private final RemotingClient remotingClient;

	public TidalClientAPIImpl(final NettyClientConfig nettyClientConfig) {
		this.remotingClient = new NettyRemotingClient(nettyClientConfig);
	}

	public void updateServerAddressList(final String addrs) {
		this.remotingClient.updateRemotingServerAddressList(parseServerAddr(addrs));
	}

	public List<String> parseServerAddr(final String addrs) {
		List<String> lst = new ArrayList<String>();
		String[] addrArray = addrs.split(",");
		for (String addr : addrArray) {
			lst.add(addr);
		}
		return lst;
	}

	public void start() {
		this.remotingClient.start();
	}

	public void shutdown() {
		this.remotingClient.shutdown();
	}

	public SendResult sendMessage( //
			final String addr, //
			final CommandCustomHeader requstHeader, //
			final long timeoutMillis, //
			final CommunicationMode communicationMode//
	) throws RemotingException, InterruptedException, TidalServerException {
		return sendMessage(addr, requstHeader, timeoutMillis, communicationMode, null);
	}

	public SendResult sendMessage( //
			final String addr, //
			final CommandCustomHeader requstHeader, //
			final long timeoutMillis, //
			final CommunicationMode communicationMode, //
			final SendCallback sendCallback //
	) throws RemotingException, InterruptedException, TidalServerException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DISPATCHER_MESSAGE, requstHeader);
		switch (communicationMode) {
		case ONEWAY:
			this.remotingClient.invokeOneway(addr, request, timeoutMillis);
			return null;
		case ASYNC:
			this.sendMessageAsync(addr, request, timeoutMillis, sendCallback);
			return null;
		case SYNC:
			return this.sendMessageSync(addr, request, timeoutMillis);
		default:
			assert false;
			break;
		}
		return null;
	}

	private SendResult sendMessageSync(//
			final String addr, //
			final RemotingCommand request, //
			final long timeoutMillis //
	) throws RemotingException, InterruptedException, TidalServerException {
		RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
		assert response != null;
		return this.processSendResponse(response);
	}

	private void sendMessageAsync(//
			final String addr, //
			final RemotingCommand request, //
			final long timeoutMillis, //
			final SendCallback sendCallback //
	) throws RemotingException, InterruptedException {
		this.remotingClient.invokeAsync(addr, request, timeoutMillis, new InvokeCallback() {

			@Override
			public void operationComplete(ResponseFuture responseFuture) {
				SendResult sendResult = null;
				try {
					sendResult = TidalClientAPIImpl.this.processSendResponse(responseFuture.getResponseCommand());
				} catch (Throwable e) {
				}
				sendCallback.onSuccess(sendResult);
			}

		});
	}

	private SendResult processSendResponse(final RemotingCommand response)
			throws TidalServerException, RemotingCommandException {
		switch (response.getCode()) {
		case ResponseCode.TOPIC_NOT_EXISTED:
		case ResponseCode.SYSTEM_ERROR:
		case ResponseCode.SYSTEM_BUSY:
		case ResponseCode.REQUEST_CODE_NOT_SUPPORTED: {
			// TODO LOG
		}
		case ResponseCode.SUCCESS: {
			SendStatus sendStatus = SendStatus.SEND_OK;
			switch (response.getCode()) {
			case ResponseCode.TOPIC_NOT_EXISTED:
				sendStatus = SendStatus.TOPIC_NOT_EXISTED;
				break;
			case ResponseCode.SYSTEM_ERROR:
				sendStatus = SendStatus.SERVER_ERROR;
				break;
			case ResponseCode.SYSTEM_BUSY:
				sendStatus = SendStatus.SERVER_BUSY;
				break;
			case ResponseCode.REQUEST_CODE_NOT_SUPPORTED:
				sendStatus = SendStatus.REQUEST_CODE_NOT_SUPPORTED;
				break;
			case ResponseCode.SUCCESS:
				sendStatus = SendStatus.SEND_OK;
				break;
			default:
				assert false;
				break;
			}
			SeatSendMsgBackRequestHeader responseHeader = //
					(SeatSendMsgBackRequestHeader) response
							.decodeCommandCustomHeader(SeatSendMsgBackRequestHeader.class);
			return new SendResult(sendStatus, responseHeader.getTopic(), responseHeader.getMessage());
		}
		default:
			break;
		}
		throw new TidalServerException(response.getCode(), response.getRemark());
	}

	public AdminResult shutdownRemotingServer( //
			final int code, //
			final String addr, //
			final CommandCustomHeader requstHeader, //
			final long timeoutMillis, //
			final CommunicationMode communicationMode//
	) throws RemotingException, InterruptedException, TidalServerException {
		return this.shutdownRemotingServer(code, addr, requstHeader, timeoutMillis, communicationMode, null);
	}

	public AdminResult shutdownRemotingServer( //
			final int code, //
			final String addr, //
			final CommandCustomHeader requstHeader, //
			final long timeoutMillis, //
			final CommunicationMode communicationMode, //
			final AdminCallback adminCallback //
	) throws RemotingException, InterruptedException, TidalServerException {
		RemotingCommand request = RemotingCommand.createRequestCommand(code, requstHeader);
		return this.defaultAdminKernelImpl(code, addr, request, timeoutMillis, communicationMode, adminCallback);
	}

	public AdminResult createTopic( //
			final int code, //
			byte[] componentData, //
			final CommandCustomHeader requstHeader, //
			final long timeoutMillis, //
			final CommunicationMode communicationMode //
	) throws RemotingException, InterruptedException, TidalServerException {
		return this.createTopic(code, componentData, requstHeader, timeoutMillis, communicationMode, null);
	}

	public AdminResult createTopic( //
			final int code, //
			byte[] componentData, //
			final CommandCustomHeader requstHeader, //
			final long timeoutMillis, //
			final CommunicationMode communicationMode, //
			final AdminCallback adminCallback //
	) throws RemotingException, InterruptedException, TidalServerException {
		RemotingCommand request = RemotingCommand.createRequestCommand(code, requstHeader);
		request.setBody(componentData);
		return this.defaultAdminKernelImpl(code, null, request, timeoutMillis, communicationMode, adminCallback);
	}

	public AdminResult queryAllTopic( //
			final int code, //
			final CommandCustomHeader requstHeader, //
			final long timeoutMillis, //
			final CommunicationMode communicationMode //
	) throws RemotingException, InterruptedException, TidalServerException {
		return this.queryAllTopic(code, requstHeader, timeoutMillis, communicationMode, null);
	}

	public AdminResult queryAllTopic( //
			final int code, //
			final CommandCustomHeader requstHeader, //
			final long timeoutMillis, //
			final CommunicationMode communicationMode, //
			final AdminCallback adminCallback //
	) throws RemotingException, InterruptedException, TidalServerException {
		RemotingCommand request = RemotingCommand.createRequestCommand(code, requstHeader);
		return this.defaultAdminKernelImpl(code, null, request, timeoutMillis, communicationMode, adminCallback);
	}

	private AdminResult defaultAdminKernelImpl( //
			final int code, //
			final String addr, //
			final RemotingCommand request, //
			final long timeoutMillis, //
			final CommunicationMode communicationMode, //
			final AdminCallback adminCallback //
	) throws RemotingException, InterruptedException, TidalServerException {
		switch (communicationMode) {
		case ONEWAY:
			this.remotingClient.invokeOneway(addr, request, timeoutMillis);
			return null;
		case ASYNC:
			this.defaultAdminAsync(code, addr, request, timeoutMillis, adminCallback);
			return null;
		case SYNC:
			return this.defaultAdminSync(code, addr, request, timeoutMillis);
		default:
			assert false;
			break;
		}
		return null;
	}

	private AdminResult defaultAdminSync( //
			final int code, //
			final String addr, //
			final RemotingCommand request, //
			final long timeoutMillis //
	) throws RemotingException, TidalServerException, InterruptedException {
		RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
		assert response != null;
		return this.processAdminResponse(code, response);
	}

	private void defaultAdminAsync(//
			final int code, //
			final String addr, //
			final RemotingCommand request, //
			final long timeoutMillis, //
			final AdminCallback adminCallback //
	) throws RemotingException, InterruptedException {
		this.remotingClient.invokeAsync(addr, request, timeoutMillis, new InvokeCallback() {

			@Override
			public void operationComplete(ResponseFuture responseFuture) {
				AdminResult adminResult = null;
				try {
					adminResult = TidalClientAPIImpl.this.processAdminResponse(code,
							responseFuture.getResponseCommand());
				} catch (Throwable e) {
				}
				adminCallback.onSuccess(adminResult);
			}

		});
	}

	private AdminResult processAdminResponse(final int code, final RemotingCommand response)
			throws RemotingCommandException, TidalServerException {
		SendStatus sendStatus = SendStatus.REQUEST_CODE_NOT_SUPPORTED;
		switch (response.getCode()) {
		case ResponseCode.TOPIC_EXISTED:
			sendStatus = SendStatus.TOPIC_EXISTED;
			break;
		case ResponseCode.PLOY_NOT_SUPPORTED:
			sendStatus = SendStatus.PLOY_NOT_SUPPORTED;
			break;
		case ResponseCode.INIT_VALUE_INVALID:
			sendStatus = SendStatus.INIT_VALUE_INVALID;
			break;
		case ResponseCode.FLUSH_DISK_FULL:
			sendStatus = SendStatus.FLUSH_DISK_FULL;
			break;
		case ResponseCode.CLASS_NOT_FOUND:
			sendStatus = SendStatus.CLASS_NOT_FOUND;
			break;
		case ResponseCode.PLOY_CLASS_MAPPING_ERROR:
			sendStatus = SendStatus.PLOY_CLASS_MAPPING_ERROR;
			break;
		case ResponseCode.PASSWORD_ERROR:
			sendStatus = SendStatus.PASSWORD_ERROR;
			break;
		case ResponseCode.SUCCESS:
			sendStatus = SendStatus.SEND_OK;
			break;
		default:
			throw new TidalServerException(response.getCode(), response.getRemark());
		}

		if (sendStatus == SendStatus.PASSWORD_ERROR) {
			return new AdminResult(sendStatus, null);
		}

		byte[] body = response.getBody();
		switch (code) {
		case RequestCode.SERVER_SHUTDOWN:
			ServerStatsData result = ServerStatsData.decode(body, ServerStatsData.class);
			return new AdminResult(sendStatus, result.getServerInfo());
		case RequestCode.CREATE_TOPIC:
			return new AdminResult(sendStatus, null);
		case RequestCode.QUERY_ALL_TOPIC:
			return new AdminResult(sendStatus, new String(body, CHARSET_UTF8));
		default:
			assert false;
			break;
		}
		return new AdminResult(sendStatus, response.getRemark());
	}

	public boolean sendHearbeat( //
			final String addr, //
			final HeartbeatData heartbeatData, //
			final long timeoutMillis //
	) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException,
			TidalServerException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
		request.setBody(heartbeatData.encode());

		RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
		assert response != null;
		switch (response.getCode()) {
		case ResponseCode.SUCCESS: {
			return true;
		}
		default:
			break;
		}
		throw new TidalServerException(response.getCode(), response.getRemark());
	}
}
