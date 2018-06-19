package tidal.client.impl.admin;

import tidal.client.AdminCallback;
import tidal.client.AdminResult;
import tidal.client.admin.DefaultAdminClient;
import tidal.client.exception.TidalServerException;
import tidal.client.impl.AbstractClientImpl;
import tidal.client.impl.CommunicationMode;
import tidal.common.protocol.RequestCode;
import tidal.common.protocol.header.AdminMsgRequestHeader;
import tidal.remoting.exception.RemotingException;

public class DefaultAdminClientImpl extends AbstractClientImpl {

	private final DefaultAdminClient defaultAdminClient;

	public DefaultAdminClientImpl(DefaultAdminClient defaultAdminClient) {
		super(defaultAdminClient);
		this.defaultAdminClient = defaultAdminClient;
	}

	public AdminResult shutdownRemotingServer(String password, String addr)
			throws RemotingException, InterruptedException, TidalServerException {
		return this.shutdownRemotingServer(password, addr, defaultAdminClient.getSendMsgTimeout());
	}

	public AdminResult shutdownRemotingServer(String password, String addr, long timeout)
			throws RemotingException, InterruptedException, TidalServerException {
		return this.shutdownRemotingServerImpl(password, addr, CommunicationMode.SYNC, null, timeout);
	}

	public void shutdownRemotingServer(String password, String addr, AdminCallback adminCallback)
			throws RemotingException, InterruptedException, TidalServerException {
		this.shutdownRemotingServer(password, addr, defaultAdminClient.getSendMsgTimeout(), adminCallback);
	}

	public void shutdownRemotingServer(String password, String addr, long timeout, AdminCallback adminCallback)
			throws RemotingException, InterruptedException, TidalServerException {
		this.shutdownRemotingServerImpl(password, addr, CommunicationMode.ASYNC, adminCallback, timeout);
	}

	private AdminResult shutdownRemotingServerImpl( //
			String password, //
			String addr, //
			final CommunicationMode communicationMode, //
			final AdminCallback adminCallback, //
			final long timeout //
	) throws RemotingException, InterruptedException, TidalServerException {
		AdminMsgRequestHeader adminMsgRequestHeader = this.createAdminRequestHeader(password);

		AdminResult adminResult = null;
		switch (communicationMode) {
		case ASYNC:
			adminResult = this.getTidalClientFactory().getTidalClientAPIImpl().shutdownRemotingServer(
					RequestCode.SERVER_SHUTDOWN, addr, adminMsgRequestHeader, timeout, communicationMode,
					adminCallback);
			break;
		case ONEWAY:
		case SYNC:
			adminResult = this.getTidalClientFactory().getTidalClientAPIImpl().shutdownRemotingServer(
					RequestCode.SERVER_SHUTDOWN, addr, adminMsgRequestHeader, timeout, communicationMode);
			break;
		default:
			assert false;
			break;
		}
		return adminResult;
	}

	public AdminResult createTopic(String password, byte[] componentData)
			throws RemotingException, InterruptedException, TidalServerException {
		return this.createTopic(password, componentData, defaultAdminClient.getSendMsgTimeout());
	}

	public AdminResult createTopic(String password, byte[] componentData, long timeout)
			throws RemotingException, InterruptedException, TidalServerException {
		return this.createTopicImpl(password, componentData, CommunicationMode.SYNC, null, timeout);
	}

	public void createTopic(String password, byte[] componentData, AdminCallback adminCallback)
			throws RemotingException, InterruptedException, TidalServerException {
		this.createTopic(password, componentData, defaultAdminClient.getSendMsgTimeout(), adminCallback);
	}

	public void createTopic(String password, byte[] componentData, long timeout, AdminCallback adminCallback)
			throws RemotingException, InterruptedException, TidalServerException {
		this.createTopicImpl(password, componentData, CommunicationMode.ASYNC, adminCallback, timeout);
	}

	private AdminResult createTopicImpl( //
			String password, //
			byte[] componentData, //
			final CommunicationMode communicationMode, //
			final AdminCallback adminCallback, //
			final long timeout //
	) throws RemotingException, InterruptedException, TidalServerException {
		AdminMsgRequestHeader adminMsgRequestHeader = this.createAdminRequestHeader(password);

		AdminResult adminResult = null;
		switch (communicationMode) {
		case ASYNC:
			adminResult = this.getTidalClientFactory().getTidalClientAPIImpl().createTopic(RequestCode.CREATE_TOPIC,
					componentData, adminMsgRequestHeader, timeout, communicationMode, adminCallback);
			break;
		case ONEWAY:
		case SYNC:
			adminResult = this.getTidalClientFactory().getTidalClientAPIImpl().createTopic(RequestCode.CREATE_TOPIC,
					componentData, adminMsgRequestHeader, timeout, communicationMode);
			break;
		default:
			assert false;
			break;
		}
		return adminResult;
	}

	public AdminResult queryAllTopic(String password)
			throws RemotingException, InterruptedException, TidalServerException {
		return this.queryAllTopic(password, defaultAdminClient.getSendMsgTimeout());
	}

	public AdminResult queryAllTopic(String password, long timeout)
			throws RemotingException, InterruptedException, TidalServerException {
		return this.queryAllTopicImpl(password, CommunicationMode.SYNC, null, timeout);
	}

	public void queryAllTopic(String password, AdminCallback adminCallback)
			throws RemotingException, InterruptedException, TidalServerException {
		this.queryAllTopic(password, defaultAdminClient.getSendMsgTimeout(), adminCallback);
	}

	public void queryAllTopic(String password, long timeout, AdminCallback adminCallback)
			throws RemotingException, InterruptedException, TidalServerException {
		this.queryAllTopicImpl(password, CommunicationMode.ASYNC, adminCallback, timeout);
	}

	private AdminResult queryAllTopicImpl( //
			String password, //
			final CommunicationMode communicationMode, //
			final AdminCallback adminCallback, //
			final long timeout //
	) throws RemotingException, InterruptedException, TidalServerException {
		AdminMsgRequestHeader adminMsgRequestHeader = this.createAdminRequestHeader(password);

		AdminResult adminResult = null;
		switch (communicationMode) {
		case ASYNC:
			adminResult = this.getTidalClientFactory().getTidalClientAPIImpl().queryAllTopic(
					RequestCode.QUERY_ALL_TOPIC, adminMsgRequestHeader, timeout, communicationMode, adminCallback);
			break;
		case ONEWAY:
		case SYNC:
			adminResult = this.getTidalClientFactory().getTidalClientAPIImpl()
					.queryAllTopic(RequestCode.QUERY_ALL_TOPIC, adminMsgRequestHeader, timeout, communicationMode);
			break;
		default:
			assert false;
			break;
		}
		return adminResult;
	}

	private AdminMsgRequestHeader createAdminRequestHeader(String password) {
		AdminMsgRequestHeader adminMsgRequestHeader = new AdminMsgRequestHeader();
		adminMsgRequestHeader.setPassword(password);
		return adminMsgRequestHeader;
	}

}
