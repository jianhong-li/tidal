package tidal.client.admin;

import tidal.client.AdminCallback;
import tidal.client.AdminResult;
import tidal.client.exception.TidalServerException;
import tidal.common.protocol.body.ComponentData;
import tidal.remoting.exception.RemotingException;

public interface AdminClient {

	AdminResult shutdownRemotingServer(String addr)
			throws RemotingException, InterruptedException, TidalServerException;

	AdminResult shutdownRemotingServer(String addr, long timeout)
			throws RemotingException, InterruptedException, TidalServerException;

	void shutdownRemotingServer(String addr, AdminCallback adminCallback)
			throws RemotingException, InterruptedException, TidalServerException;

	void shutdownRemotingServer(String addr, long timeout, AdminCallback adminCallback)
			throws RemotingException, InterruptedException, TidalServerException;

	AdminResult createTopic(ComponentData componentData)
			throws RemotingException, InterruptedException, TidalServerException;

	AdminResult createTopic(ComponentData componentData, long timeout)
			throws RemotingException, InterruptedException, TidalServerException;

	void createTopic(ComponentData componentData, AdminCallback adminCallback)
			throws RemotingException, InterruptedException, TidalServerException;

	void createTopic(ComponentData componentData, long timeout, AdminCallback adminCallback)
			throws RemotingException, InterruptedException, TidalServerException;

	AdminResult queryAllTopic() throws RemotingException, InterruptedException, TidalServerException;

	AdminResult queryAllTopic(long timeout) throws RemotingException, InterruptedException, TidalServerException;

	void queryAllTopic(AdminCallback adminCallback)
			throws RemotingException, InterruptedException, TidalServerException;

	void queryAllTopic(long timeout, AdminCallback adminCallback)
			throws RemotingException, InterruptedException, TidalServerException;
}
