package tidal.store;

import java.util.List;

import tidal.common.protocol.body.ComponentData;

public interface TidalStore {

	void start() throws Exception;

	void shutdown();

	void updateHaMasterAddress(final String newAddr);
	
	List<ComponentData> queryAllTopic();

	int putComponent(String reqId, int ploy, int initValue, String className);

	String updateComponent(String reqId);

	boolean isOSPageCacheBusy();
}
