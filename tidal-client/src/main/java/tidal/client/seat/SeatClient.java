package tidal.client.seat;

import tidal.client.SendCallback;
import tidal.client.SendResult;
import tidal.client.exception.TidalServerException;
import tidal.remoting.exception.RemotingException;

public interface SeatClient {

	SendResult send(String topic) throws RemotingException, InterruptedException, TidalServerException;

	SendResult send(String topic, long timeout) throws RemotingException, InterruptedException, TidalServerException;

	void send(String topic, SendCallback sendCallback)
			throws RemotingException, InterruptedException, TidalServerException;

	void send(String topic, SendCallback sendCallback, long timeout)
			throws RemotingException, InterruptedException, TidalServerException;
}
