package tidal.store.config;

import java.io.File;

import tidal.common.MixAll;
import tidal.common.annotation.ImportantField;

public class StoreConfig {

	public static final String TIDAL_STORE_DEFAULT_CHARSET = "UTF-8";
	public static final String TIDAL_STORE_FILE_NAME = "tidal";

	@ImportantField
	private String haServerAddr = System.getProperty(MixAll.TIDAL_ADDR_PROPERTY, System.getenv(MixAll.TIDAL_ADDR_ENV));

	private boolean isCluster = false;
	@ImportantField
	private int haListenPort = 10737;
	private int haSendHeartbeatInterval = 1000 * 5;
	private int haHousekeepingInterval = 1000 * 20;
	private long osPageCacheBusyTimeOutMills = 1000;

	@ImportantField
	private String storePath = System.getProperty("user.home");

	public String getHaServerAddr() {
		return haServerAddr;
	}

	public void setHaServerAddr(String haServerAddr) {
		this.haServerAddr = haServerAddr;
	}

	public boolean isCluster() {
		return isCluster;
	}

	public void setCluster(boolean isCluster) {
		this.isCluster = isCluster;
	}

	public int getHaListenPort() {
		return haListenPort;
	}

	public void setHaListenPort(int haListenPort) {
		this.haListenPort = haListenPort;
	}

	public int getHaSendHeartbeatInterval() {
		return haSendHeartbeatInterval;
	}

	public void setHaSendHeartbeatInterval(int haSendHeartbeatInterval) {
		this.haSendHeartbeatInterval = haSendHeartbeatInterval;
	}

	public int getHaHousekeepingInterval() {
		return haHousekeepingInterval;
	}

	public void setHaHousekeepingInterval(int haHousekeepingInterval) {
		this.haHousekeepingInterval = haHousekeepingInterval;
	}

	public long getOsPageCacheBusyTimeOutMills() {
		return osPageCacheBusyTimeOutMills;
	}

	public void setOsPageCacheBusyTimeOutMills(final long osPageCacheBusyTimeOutMills) {
		this.osPageCacheBusyTimeOutMills = osPageCacheBusyTimeOutMills;
	}

	public String getStorePath() {
		return storePath;
	}

	public void setStorePath(String storePath) {
		this.storePath = storePath;
	}

	public String getStoreRootDir() {
		return storePath + File.separator + ".tidal";
	}

}
