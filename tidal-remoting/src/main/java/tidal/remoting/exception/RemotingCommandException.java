package tidal.remoting.exception;

public class RemotingCommandException extends RemotingException {

	private static final long serialVersionUID = 919429244549792447L;

	public RemotingCommandException(String message) {
		super(message, null);
	}

	public RemotingCommandException(String message, Throwable cause) {
		super(message, cause);
	}
}