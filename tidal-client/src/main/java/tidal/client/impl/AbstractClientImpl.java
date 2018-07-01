package tidal.client.impl;

import tidal.client.AbstractTidalClient;
import tidal.client.exception.TidalClientException;
import tidal.client.impl.factory.TidalClientInstance;
import tidal.common.ServiceState;

public abstract class AbstractClientImpl {

	private final AbstractTidalClient abstractTidalClient;
	private ServiceState serviceState = ServiceState.CREATE_JUST;
	private TidalClientInstance tidalClientFactory;

	public AbstractClientImpl(final AbstractTidalClient abstractTidalClient) {
		this.abstractTidalClient = abstractTidalClient;
	}

	public void start() throws TidalClientException {
		switch (this.serviceState) {
		case CREATE_JUST:
			this.serviceState = ServiceState.START_FAILED;
			this.tidalClientFactory = TidalClientManager.getInstance()
					.getAndCreateClientInstance(this.abstractTidalClient);

			boolean registerOK = tidalClientFactory.registerClient(abstractTidalClient.getClientGroup(), this);
			if (!registerOK) {
				this.serviceState = ServiceState.CREATE_JUST;
				throw new TidalClientException(
						"The client group[" + this.abstractTidalClient.getClientGroup() + "] has been created before.",
						null);
			}

			tidalClientFactory.start();
			this.serviceState = ServiceState.RUNNING;
			break;
		case RUNNING:
		case START_FAILED:
		case SHUTDOWN_ALREADY:
			throw new TidalClientException("The service state not OK, maybe started once, ", null);
		default:
			break;
		}
	}

	public void shutdown() {
		switch (this.serviceState) {
		case CREATE_JUST:
			break;
		case RUNNING:
			this.tidalClientFactory.unregisterClient(abstractTidalClient.getClientGroup());
			this.tidalClientFactory.shutdown();
			this.serviceState = ServiceState.SHUTDOWN_ALREADY;
			break;
		case SHUTDOWN_ALREADY:
			break;
		default:
			break;
		}
	}

	public TidalClientInstance getTidalClientFactory() {
		return tidalClientFactory;
	}

}
