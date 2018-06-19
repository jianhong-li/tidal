package tidal.common.protocol.header;

import tidal.remoting.CommandCustomHeader;
import tidal.remoting.annotation.CFNotNull;
import tidal.remoting.exception.RemotingCommandException;

public class AdminMsgRequestHeader implements CommandCustomHeader {

	@CFNotNull
	private String password;

	@Override
	public void checkFields() throws RemotingCommandException {
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

}
