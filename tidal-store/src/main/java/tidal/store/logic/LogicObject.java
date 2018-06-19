package tidal.store.logic;

public class LogicObject<T> {

	private String topic;
	private Class<T> cls;
	private T t;

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public Class<T> getCls() {
		return cls;
	}

	public void setCls(Class<T> cls) {
		this.cls = cls;
	}

	public T getT() {
		return t;
	}

	public void setT(T t) {
		this.t = t;
	}
}
