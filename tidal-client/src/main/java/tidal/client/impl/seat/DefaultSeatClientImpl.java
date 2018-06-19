package tidal.client.impl.seat;

import tidal.client.SendCallback;
import tidal.client.SendResult;
import tidal.client.exception.TidalServerException;
import tidal.client.impl.AbstractClientImpl;
import tidal.client.impl.CommunicationMode;
import tidal.client.seat.DefaultSeatClient;
import tidal.common.protocol.header.SeatSendMsgBackRequestHeader;
import tidal.remoting.exception.RemotingException;

public class DefaultSeatClientImpl extends AbstractClientImpl {

	private final DefaultSeatClient defaultSeatClient;

	public DefaultSeatClientImpl(final DefaultSeatClient defaultSeatClient) {
		super(defaultSeatClient);
		this.defaultSeatClient = defaultSeatClient;
	}

	public SendResult send(String topic) throws RemotingException, InterruptedException, TidalServerException {
		return this.send(topic, defaultSeatClient.getSendMsgTimeout());
	}

	public SendResult send(String topic, long timeout)
			throws RemotingException, InterruptedException, TidalServerException {
		return this.sendDefaultImpl(topic, CommunicationMode.SYNC, null, timeout);
	}

	public void send(String topic, SendCallback sendCallback)
			throws RemotingException, InterruptedException, TidalServerException {
		this.send(topic, sendCallback, defaultSeatClient.getSendMsgTimeout());
	}

	public void send(String topic, SendCallback sendCallback, long timeout)
			throws RemotingException, InterruptedException, TidalServerException {
		this.sendDefaultImpl(topic, CommunicationMode.ASYNC, sendCallback, timeout);
	}

	private SendResult sendDefaultImpl( //
			String topic, //
			final CommunicationMode communicationMode, //
			final SendCallback sendCallback, //
			final long timeout //
	) throws RemotingException, InterruptedException, TidalServerException {
		SeatSendMsgBackRequestHeader seatSendMsgBackRequestHeader = new SeatSendMsgBackRequestHeader();
		seatSendMsgBackRequestHeader.setTopic(topic);

		SendResult sendResult = null;
		switch (communicationMode) {
		case ASYNC:
			sendResult = this.getTidalClientFactory().getTidalClientAPIImpl().sendMessage(null,
					seatSendMsgBackRequestHeader, timeout, communicationMode, sendCallback);
			break;
		case ONEWAY:
		case SYNC:
			sendResult = this.getTidalClientFactory().getTidalClientAPIImpl().sendMessage(null,
					seatSendMsgBackRequestHeader, timeout, communicationMode);
			break;
		default:
			assert false;
			break;
		}
		return sendResult;
	}

}
