package tidal.client;


import tidal.common.MixAll;
import tidal.common.UtilAll;
import tidal.remoting.common.RemotingHelper;

public class ClientConfig {

	private String serverAddr = System.getProperty(MixAll.TIDAL_ADDR_PROPERTY, System.getenv(MixAll.TIDAL_ADDR_ENV));

	private String clientIP = RemotingHelper.getPhyLocalAddress();
	private String instanceName = System.getProperty("tidal.client.name", "DEFAULT");
	private int heartbeatBrokerInterval = 1000 * 30;
	private boolean unitMode = false;
    private String unitName;
	
	public String buildClientId() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClientIP());

        sb.append("@");
        sb.append(this.getInstanceName());
        if (!UtilAll.isBlank(this.unitName)) {
            sb.append("@");
            sb.append(this.unitName);
        }

        return sb.toString();
    }

	public ClientConfig cloneClientConfig() {
        ClientConfig cc = new ClientConfig();
        cc.serverAddr = serverAddr;
        cc.clientIP = clientIP;
        cc.instanceName = instanceName;
        cc.heartbeatBrokerInterval = heartbeatBrokerInterval;
        cc.unitMode = unitMode;
        cc.unitName = unitName;
        return cc;
    }
	
    public String getClientIP() {
        return clientIP;
    }

    public void setClientIP(String clientIP) {
        this.clientIP = clientIP;
    }

	public String getServerAddr() {
		return serverAddr;
	}

	public void setServerAddr(String serverAddr) {
		this.serverAddr = serverAddr;
	}

	public String getInstanceName() {
		return instanceName;
	}

	public void setInstanceName(String instanceName) {
		this.instanceName = instanceName;
	}

	public int getHeartbeatBrokerInterval() {
		return heartbeatBrokerInterval;
	}

	public void setHeartbeatBrokerInterval(int heartbeatBrokerInterval) {
		this.heartbeatBrokerInterval = heartbeatBrokerInterval;
	}

	public boolean isUnitMode() {
		return unitMode;
	}

	public void setUnitMode(boolean unitMode) {
		this.unitMode = unitMode;
	}

	public String getUnitName() {
		return unitName;
	}

	public void setUnitName(String unitName) {
		this.unitName = unitName;
	}
    
}
