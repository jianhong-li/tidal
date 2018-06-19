package tidal.client;

public class AdminResult {

	private SendStatus sendStatus;
	private String result;

	public AdminResult(SendStatus sendStatus, String result) {
		super();
		this.sendStatus = sendStatus;
		this.result = result;
	}

	public SendStatus getSendStatus() {
		return sendStatus;
	}

	public void setSendStatus(SendStatus sendStatus) {
		this.sendStatus = sendStatus;
	}

	public String getResult() {
		return result;
	}

	public void setResult(String result) {
		this.result = result;
	}

	@Override
	public String toString() {
		return "AdminResult [sendStatus=" + sendStatus + ", result=" + result + "]";
	}

}
