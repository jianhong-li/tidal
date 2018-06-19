package tidal.client.admin;

import tidal.client.AbstractTidalClient;
import tidal.client.AdminCallback;
import tidal.client.AdminResult;
import tidal.client.exception.TidalClientException;
import tidal.client.exception.TidalServerException;
import tidal.client.impl.admin.DefaultAdminClientImpl;
import tidal.common.protocol.body.ComponentData;
import tidal.remoting.exception.RemotingException;

/**
 * 
 * This class is the entry point for applications intending to manage topic.
 * 
 */
public class DefaultAdminClient extends AbstractTidalClient implements AdminClient {

	protected final transient DefaultAdminClientImpl defaultAdminClientImpl;

	/**
	 * auth client
	 */
	private String password;
	/**
	 * The group has DefaultAdminClient and DefaultSeatClient.
	 */
	private String clientGroup;
	/**
	 * Timeout for sending messages.
	 */
	private int sendMsgTimeout = 3000;

	/**
	 * Default constructor.
	 */
	public DefaultAdminClient() {
		this.defaultAdminClientImpl = new DefaultAdminClientImpl(this);
	}

	/**
	 * Start this AdminClient instance.
	 * </p>
	 *
	 * <strong>
	 * Much internal initializing procedures are carried out to make this instance prepared, thus, it's a must to invoke
	 * this method before sending or querying messages.
	 * </strong>
	 * </p>
	 *
	 * @throws TidalClientException if there is any unexpected error.
	 */
	@Override
	public void start() throws TidalClientException {
		this.defaultAdminClientImpl.start();
	}

	/**
	 * This method shuts down this AdminClient instance and releases related resources.
	 */
	@Override
	public void shutdown() {
		defaultAdminClientImpl.shutdown();
	}

	/**
	 * Shutdown remoting server in synchronous mode. This method returns only when the sending procedure totally completes.
	 *
	 * @param java.lang.String addr to send.
	 * @return {@link AdminResult} instance to inform senders details of the deliverable, say result of the message,
	 * {@link AdminResult} indicating message status, etc.
	 * @throws RemotingException if there is any network-tier error.
	 * @throws TidalServerException if there is any error with broker.
	 * @throws InterruptedException if the sending thread is interrupted.
	 */
	@Override
	public AdminResult shutdownRemotingServer(String addr)
			throws RemotingException, InterruptedException, TidalServerException {
		return this.defaultAdminClientImpl.shutdownRemotingServer(password, addr);
	}

	/**
	 * Same to {@link #shutdownRemotingServer(java.lang.String)} with send timeout specified in addition.
	 *
	 * @param java.lang.String addr to send.
	 * @param timeout send timeout.
	 * @return {@link AdminResult} instance to inform senders details of the deliverable, say result of the message,
	 * {@link AdminResult} indicating message status, etc.
	 * @throws RemotingException if there is any network-tier error.
	 * @throws TidalServerException if there is any error with broker.
	 * @throws InterruptedException if the sending thread is interrupted.
	 */
	@Override
	public AdminResult shutdownRemotingServer(String addr, long timeout)
			throws RemotingException, InterruptedException, TidalServerException {
		return this.defaultAdminClientImpl.shutdownRemotingServer(password, addr, timeout);
	}

	/**
	 * Shutdown remoting server asynchronously.
	 * </p>
	 *
	 * This method returns immediately. On sending completion, <code>sendCallback</code> will be executed.
	 * </p>
	 * 
	 * @param java.lang.String addr to send.
	 * @param sendCallback Callback to execute on sending completed, either successful or unsuccessful.
	 * @throws RemotingException if there is any network-tier error.
	 * @throws TidalServerException if there is any error with broker.
	 * @throws InterruptedException if the sending thread is interrupted.
	 */
	@Override
	public void shutdownRemotingServer(String addr, AdminCallback adminCallback)
			throws RemotingException, InterruptedException, TidalServerException {
		this.defaultAdminClientImpl.shutdownRemotingServer(password, addr, adminCallback);
	}

	/**
	 * Same to {@link #shutdownRemotingServer(java.lang.String, AdminCallback)} with send timeout specified in addition.
	 * 
	 * @param java.lang.String addr to send.
	 * @param timeout send timeout.
	 * @param sendCallback Callback to execute on sending completed, either successful or unsuccessful.
	 * @throws RemotingException if there is any network-tier error.
	 * @throws TidalServerException if there is any error with broker.
	 * @throws InterruptedException if the sending thread is interrupted.
	 */
	@Override
	public void shutdownRemotingServer(String addr, long timeout, AdminCallback adminCallback)
			throws RemotingException, InterruptedException, TidalServerException {
		this.defaultAdminClientImpl.shutdownRemotingServer(password, addr, timeout, adminCallback);
	}

	/**
	 * Create topic in synchronous mode. This method returns only when the sending procedure totally completes.
	 *
	 * @param ComponentData componentData to send.
	 * @return {@link AdminResult} instance to inform senders details of the deliverable, say result of the message,
	 * {@link AdminResult} indicating message status, etc.
	 * @throws RemotingException if there is any network-tier error.
	 * @throws TidalServerException if there is any error with broker.
	 * @throws InterruptedException if the sending thread is interrupted.
	 */
	@Override
	public AdminResult createTopic(ComponentData componentData)
			throws RemotingException, InterruptedException, TidalServerException {
		return this.defaultAdminClientImpl.createTopic(password, componentData.encode(true));
	}

	/**
	 * Same to {@link #createTopic(ComponentData)} with send timeout specified in addition.
	 *
	 * @param ComponentData componentData to send.
	 * @param timeout send timeout.
	 * @return {@link AdminResult} instance to inform senders details of the deliverable, say result of the message,
	 * {@link AdminResult} indicating message status, etc.
	 * @throws RemotingException if there is any network-tier error.
	 * @throws TidalServerException if there is any error with broker.
	 * @throws InterruptedException if the sending thread is interrupted.
	 */
	@Override
	public AdminResult createTopic(ComponentData componentData, long timeout)
			throws RemotingException, InterruptedException, TidalServerException {
		return this.defaultAdminClientImpl.createTopic(password, componentData.encode(true));
	}

	/**
	 * Create topic asynchronously.
	 * </p>
	 *
	 * This method returns immediately. On sending completion, <code>sendCallback</code> will be executed.
	 * </p>
	 *
	 * @param ComponentData componentData to send.
	 * @param sendCallback Callback to execute on sending completed, either successful or unsuccessful.
	 * @throws RemotingException if there is any network-tier error.
	 * @throws TidalServerException if there is any error with broker.
	 * @throws InterruptedException if the sending thread is interrupted.
	 */
	@Override
	public void createTopic(ComponentData componentData, AdminCallback adminCallback)
			throws RemotingException, InterruptedException, TidalServerException {
		this.defaultAdminClientImpl.createTopic(password, componentData.encode(true));
	}

	/**
	 * Same to {@link #createTopic(ComponentData, AdminCallback)} with send timeout specified in addition.
	 *
	 * @param ComponentData componentData to send.
	 * @param timeout send timeout.
	 * @param sendCallback Callback to execute on sending completed, either successful or unsuccessful.
	 * @throws RemotingException if there is any network-tier error.
	 * @throws TidalServerException if there is any error with broker.
	 * @throws InterruptedException if the sending thread is interrupted.
	 */
	@Override
	public void createTopic(ComponentData componentData, long timeout, AdminCallback adminCallback)
			throws RemotingException, InterruptedException, TidalServerException {
		this.defaultAdminClientImpl.createTopic(password, componentData.encode(true));
	}

	/**
	 * Query all topic in synchronous mode. This method returns only when the sending procedure totally completes.
	 *
	 * @return {@link AdminResult} instance to inform senders details of the deliverable, say result list of the message,
	 * {@link AdminResult} indicating message status, etc.
	 * @throws RemotingException if there is any network-tier error.
	 * @throws TidalServerException if there is any error with broker.
	 * @throws InterruptedException if the sending thread is interrupted.
	 */
	@Override
	public AdminResult queryAllTopic() throws RemotingException, InterruptedException, TidalServerException {
		return this.defaultAdminClientImpl.queryAllTopic(password);
	}

	/**
	 * Same to {@link #queryAllTopic()} with send timeout specified in addition.
	 * 
	 * @param timeout send timeout.
	 * @return {@link AdminResult} instance to inform senders details of the deliverable, say result list of the message,
	 * {@link AdminResult} indicating message status, etc.
	 * @throws RemotingException if there is any network-tier error.
	 * @throws TidalServerException if there is any error with broker.
	 * @throws InterruptedException if the sending thread is interrupted.
	 */
	@Override
	public AdminResult queryAllTopic(long timeout)
			throws RemotingException, InterruptedException, TidalServerException {
		return this.defaultAdminClientImpl.queryAllTopic(password, timeout);
	}

	/**
	 * Query all topic asynchronously.
	 * </p>
	 *
	 * This method returns immediately. On sending completion, <code>sendCallback</code> will be executed.
	 * </p>
	 * 
	 * @param sendCallback Callback to execute on sending completed, either successful or unsuccessful.
	 * @throws RemotingException if there is any network-tier error.
	 * @throws TidalServerException if there is any error with broker.
	 * @throws InterruptedException if the sending thread is interrupted.
	 */
	@Override
	public void queryAllTopic(AdminCallback adminCallback)
			throws RemotingException, InterruptedException, TidalServerException {
		this.defaultAdminClientImpl.queryAllTopic(password, adminCallback);
	}

	/**
	 * Same to {@link #queryAllTopic(AdminCallback)} with send timeout specified in addition.
	 * 
	 * @param timeout send timeout.
	 * @param sendCallback Callback to execute on sending completed, either successful or unsuccessful.
	 * @throws RemotingException if there is any network-tier error.
	 * @throws TidalServerException if there is any error with broker.
	 * @throws InterruptedException if the sending thread is interrupted.
	 */
	@Override
	public void queryAllTopic(long timeout, AdminCallback adminCallback)
			throws RemotingException, InterruptedException, TidalServerException {
		this.defaultAdminClientImpl.queryAllTopic(password, timeout, adminCallback);
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public void setClientGroup(String clientGroup) {
		this.clientGroup = clientGroup;
	}

	public int getSendMsgTimeout() {
		return sendMsgTimeout;
	}

	public void setSendMsgTimeout(int sendMsgTimeout) {
		this.sendMsgTimeout = sendMsgTimeout;
	}

	@Override
	public String getClientGroup() {
		return clientGroup == null ? DefaultAdminClient.class.getSimpleName() : clientGroup;
	}

}
