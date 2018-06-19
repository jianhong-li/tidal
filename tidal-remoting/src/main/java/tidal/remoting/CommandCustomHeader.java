package tidal.remoting;

import tidal.remoting.exception.RemotingCommandException;

public interface CommandCustomHeader {
	void checkFields() throws RemotingCommandException;
}
