package tidal.client.seat;

import tidal.client.AbstractTidalClient;
import tidal.client.SendCallback;
import tidal.client.SendResult;
import tidal.client.SendStatus;
import tidal.client.exception.TidalClientException;
import tidal.client.exception.TidalServerException;
import tidal.client.impl.seat.DefaultSeatClientImpl;
import tidal.remoting.exception.RemotingException;

public class DefaultSeatClient extends AbstractTidalClient implements SeatClient {

	protected final transient DefaultSeatClientImpl defaultSeatClientImpl;
	/**
	 * The group has DefaultSeatClient and DefaultAdminClient.
	 */
	private String clientGroup;
	/**
	 * Timeout for sending messages.
	 */
	private int sendMsgTimeout = 3000;

	/**
	 * Default constructor.
	 */
	public DefaultSeatClient() {
		this.defaultSeatClientImpl = new DefaultSeatClientImpl(this);
	}

	/**
	 * Start this SeatClient instance.
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
		this.defaultSeatClientImpl.start();
	}

	/**
	 * This method shuts down this SeatClient instance and releases related resources.
	 */
	@Override
	public void shutdown() {
		this.defaultSeatClientImpl.shutdown();
	}

	/**
	 * Send message in synchronous mode. This method returns only when the sending procedure totally completes.
	 *
	 * @param java.lang.String topic to send.
	 * @return {@link SendResult} instance to inform senders details of the deliverable, say topic of the message,
	 * {@link SendStatus} indicating message status, etc.
	 * @throws RemotingException if there is any network-tier error.
	 * @throws TidalServerException if there is any error with broker.
	 * @throws InterruptedException if the sending thread is interrupted.
	 */
	@Override
	public SendResult send(String topic) throws RemotingException, InterruptedException, TidalServerException {
		return defaultSeatClientImpl.send(topic);
	}

	/**
	 * Same to {@link #send(java.lang.String)} with send timeout specified in addition.
	 * @param java.lang.String topic to send.
	 * @param timeout send timeout.
	 * @return {@link SendResult} instance to inform senders details of the deliverable, say topic of the message,
	 * {@link SendStatus} indicating message status, etc.
	 * @throws RemotingException if there is any network-tier error.
	 * @throws TidalServerException if there is any error with broker.
	 * @throws InterruptedException if the sending thread is interrupted.
	 */
	@Override
	public SendResult send(String topic, long timeout)
			throws RemotingException, InterruptedException, TidalServerException {
		return defaultSeatClientImpl.send(topic, timeout);
	}

	/**
	 * Send message to server asynchronously.
	 * </p>
	 *
	 * This method returns immediately. On sending completion, <code>sendCallback</code> will be executed.
	 * </p>
	 * 
	 * @param java.lang.String topic to send.
	 * @param sendCallback Callback to execute on sending completed, either successful or unsuccessful.
	 * @throws RemotingException if there is any network-tier error.
	 * @throws TidalServerException if there is any error with broker.
	 * @throws InterruptedException if the sending thread is interrupted.
	 */
	@Override
	public void send(String topic, SendCallback sendCallback)
			throws RemotingException, InterruptedException, TidalServerException {
		defaultSeatClientImpl.send(topic, sendCallback);
	}

	/**
	 * Same to {@link #send(java.lang.String, SendCallback)} with send timeout specified in addition.
	 * 
	 * @param java.lang.String topic to send.
	 * @param sendCallback Callback to execute on sending completed, either successful or unsuccessful.
	 * @param timeout send timeout.
	 * @throws RemotingException if there is any network-tier error.
	 * @throws TidalServerException if there is any error with broker.
	 * @throws InterruptedException if the sending thread is interrupted.
	 */
	@Override
	public void send(String topic, SendCallback sendCallback, long timeout)
			throws RemotingException, InterruptedException, TidalServerException {
		defaultSeatClientImpl.send(topic, sendCallback, timeout);
	}

	public DefaultSeatClientImpl getDefaultSeatClientImpl() {
		return defaultSeatClientImpl;
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
		return clientGroup == null ? DefaultSeatClient.class.getSimpleName() : clientGroup;
	}

}
