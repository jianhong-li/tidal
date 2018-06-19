package tidal.client;

public class SendResult {

	private SendStatus sendStatus;
	private String topic;
	private String message;

	public SendResult(SendStatus sendStatus, String topic, String message) {
		super();
		this.sendStatus = sendStatus;
		this.topic = topic;
		this.message = message;
	}

	public SendStatus getSendStatus() {
		return sendStatus;
	}

	public void setSendStatus(SendStatus sendStatus) {
		this.sendStatus = sendStatus;
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

	@Override
	public String toString() {
		return "SendResult [sendStatus=" + sendStatus + ", topic=" + topic + ", message=" + message + "]";
	}

}
