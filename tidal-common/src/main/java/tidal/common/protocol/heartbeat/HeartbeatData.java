package tidal.common.protocol.heartbeat;

import tidal.remoting.protocol.RemotingSerializable;

public class HeartbeatData extends RemotingSerializable {
	private String clientID;

	public String getClientID() {
		return clientID;
	}

	public void setClientID(String clientID) {
		this.clientID = clientID;
	}

	@Override
	public String toString() {
		return "HeartbeatData [clientID=" + clientID + "]";
	}
}
