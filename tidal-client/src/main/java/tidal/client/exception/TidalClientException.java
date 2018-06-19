package tidal.client.exception;

public class TidalClientException extends Exception {

	private static final long serialVersionUID = -7001546378694416995L;
	
	private int responseCode;
    private String errorMessage;

	public TidalClientException(String errorMessage, Throwable cause) {
		super(errorMessage, cause);
	}

	public int getResponseCode() {
		return responseCode;
	}

	public String getErrorMessage() {
		return errorMessage;
	}
	
}
