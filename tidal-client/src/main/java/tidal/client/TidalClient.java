package tidal.client;

import tidal.client.exception.TidalClientException;

public interface TidalClient {

	void start() throws TidalClientException;

	void shutdown();

}
