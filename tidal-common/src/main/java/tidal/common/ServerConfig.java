package tidal.common;

import tidal.common.annotation.ImportantField;

public class ServerConfig {

	@ImportantField
	private String tidalHome = System.getProperty(MixAll.TIDAL_HOME_PROPERTY, System.getenv(MixAll.TIDAL_HOME_ENV));

	private String password = "foobared";
	private int dispatcherMessageThreadPoolNums = 16 + Runtime.getRuntime().availableProcessors() * 2;
	private int clientManageThreadPoolNums = 32;
	private int adminManageThreadPoolNums = 1;
	private int dispatcherThreadPoolQueueCapacity = 100000;
	private int clientManagerThreadPoolQueueCapacity = 1000;
	private int adminManagerThreadPoolQueueCapacity = 10;

	public String getTidalHome() {
		return tidalHome;
	}

	public void setTidalHome(String tidalHome) {
		this.tidalHome = tidalHome;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public int getDispatcherMessageThreadPoolNums() {
		return dispatcherMessageThreadPoolNums;
	}

	public void setDispatcherMessageThreadPoolNums(int dispatcherMessageThreadPoolNums) {
		this.dispatcherMessageThreadPoolNums = dispatcherMessageThreadPoolNums;
	}

	public int getClientManageThreadPoolNums() {
		return clientManageThreadPoolNums;
	}

	public void setClientManageThreadPoolNums(int clientManageThreadPoolNums) {
		this.clientManageThreadPoolNums = clientManageThreadPoolNums;
	}

	public int getAdminManageThreadPoolNums() {
		return adminManageThreadPoolNums;
	}

	public void setAdminManageThreadPoolNums(int adminManageThreadPoolNums) {
		this.adminManageThreadPoolNums = adminManageThreadPoolNums;
	}

	public int getDispatcherThreadPoolQueueCapacity() {
		return dispatcherThreadPoolQueueCapacity;
	}

	public void setDispatcherThreadPoolQueueCapacity(int dispatcherThreadPoolQueueCapacity) {
		this.dispatcherThreadPoolQueueCapacity = dispatcherThreadPoolQueueCapacity;
	}

	public int getClientManagerThreadPoolQueueCapacity() {
		return clientManagerThreadPoolQueueCapacity;
	}

	public void setClientManagerThreadPoolQueueCapacity(int clientManagerThreadPoolQueueCapacity) {
		this.clientManagerThreadPoolQueueCapacity = clientManagerThreadPoolQueueCapacity;
	}

	public int getAdminManagerThreadPoolQueueCapacity() {
		return adminManagerThreadPoolQueueCapacity;
	}

	public void setAdminManagerThreadPoolQueueCapacity(int adminManagerThreadPoolQueueCapacity) {
		this.adminManagerThreadPoolQueueCapacity = adminManagerThreadPoolQueueCapacity;
	}

}
