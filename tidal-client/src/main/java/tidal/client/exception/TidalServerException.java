package tidal.client.exception;

import tidal.common.UtilAll;

public class TidalServerException extends Exception {

	private static final long serialVersionUID = -8898576085657162290L;

	private final int responseCode;
	private final String errorMessage;

	public TidalServerException(int responseCode, String errorMessage) {
		super("CODE: " + UtilAll.responseCode2String(responseCode) + "  DESC: " + errorMessage);
		this.responseCode = responseCode;
		this.errorMessage = errorMessage;
	}

	public int getResponseCode() {
		return responseCode;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

}
