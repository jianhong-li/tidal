package tidal.common.protocol.body;

import tidal.remoting.protocol.RemotingSerializable;

public class ServerStatsData extends RemotingSerializable {

	private String serverInfo;

	public String getServerInfo() {
		return serverInfo;
	}

	public void setServerInfo(String serverInfo) {
		this.serverInfo = serverInfo;
	}

}
