package tidal.common.protocol.header;


import tidal.remoting.CommandCustomHeader;
import tidal.remoting.annotation.CFNotNull;
import tidal.remoting.annotation.CFNullable;
import tidal.remoting.exception.RemotingCommandException;

public class SeatSendMsgBackRequestHeader implements CommandCustomHeader {

	@CFNotNull
	private String topic;
	@CFNullable
	private String message;

	@Override
	public void checkFields() throws RemotingCommandException {
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

}
