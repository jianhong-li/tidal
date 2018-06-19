package tidal.remoting;

import java.util.List;

import tidal.remoting.exception.RemotingConnectException;
import tidal.remoting.exception.RemotingSendRequestException;
import tidal.remoting.exception.RemotingTimeoutException;
import tidal.remoting.exception.RemotingTooMuchRequestException;
import tidal.remoting.protocol.RemotingCommand;

public interface RemotingClient extends RemotingService {
	
	public void updateRemotingServerAddressList(final List<String> addrs);

    public List<String> getRemotingServerAddressList();

	public RemotingCommand invokeSync(final String addr, final RemotingCommand request, final long timeoutMillis)
			throws InterruptedException, RemotingConnectException, RemotingSendRequestException,
			RemotingTimeoutException;

	public void invokeAsync(final String addr, final RemotingCommand request, final long timeoutMillis,
	        final InvokeCallback invokeCallback) throws InterruptedException, RemotingConnectException,
	        RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

	public void invokeOneway(final String addr, final RemotingCommand request, final long timeoutMillis)
			throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException,
			RemotingTimeoutException, RemotingSendRequestException;
			
	public boolean isChannelWriteable(final String addr);
}
