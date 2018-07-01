package tidal.client.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tidal.client.AbstractTidalClient;
import tidal.client.impl.factory.TidalClientInstance;
import tidal.common.constant.LoggerName;

public class TidalClientManager {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.CLIENT_LOGGER_NAME);
	private AtomicInteger factoryIndexGenerator = new AtomicInteger();
	private ConcurrentMap<String/* clientId */, TidalClientInstance> factoryTable = //
			new ConcurrentHashMap<String, TidalClientInstance>();

	private static class SingletonHolder {
		private static final TidalClientManager INSTANCE = new TidalClientManager();
	}

	private TidalClientManager() {
	}

	public static final TidalClientManager getInstance() {
		return SingletonHolder.INSTANCE;
	}

	public TidalClientInstance getAndCreateClientInstance(final AbstractTidalClient abstractTidalClient) {
		String clientId = abstractTidalClient.buildClientId();
		TidalClientInstance instance = this.factoryTable.get(clientId);
		if (null == instance) {
			instance = new TidalClientInstance(abstractTidalClient, this.factoryIndexGenerator.getAndIncrement(),
					clientId);
			TidalClientInstance prev = this.factoryTable.putIfAbsent(clientId, instance);
			if (prev != null) {
				instance = prev;
				log.warn("Returned Previous TidalClientInstance for clientId:[{}]", clientId);
			} else {
				log.info("Created new TidalClientInstance for clientId:[{}]", clientId);
			}
		}

		return instance;
	}

	public void removeClientFactory(final String clientId) {
		this.factoryTable.remove(clientId);
	}

}
