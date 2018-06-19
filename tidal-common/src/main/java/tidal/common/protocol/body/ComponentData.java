package tidal.common.protocol.body;

import tidal.remoting.protocol.RemotingSerializable;

public class ComponentData extends RemotingSerializable {

	private String topic;
	private int ploy;
	private int initValue;
	private String className;

	public static ComponentData decode(final byte[] data, boolean nullCheck) {
		ComponentData decodeComponentData = decode(data, ComponentData.class);
		if (!nullCheck) {
			return decodeComponentData;
		}
		if (decodeComponentData.isEmpty(decodeComponentData.getTopic())
				|| decodeComponentData.isEmpty(decodeComponentData.getClassName())) {
			return null;
		} else {
			return decodeComponentData;
		}
	}

	public byte[] encode(boolean nullCheck) {
		if (!nullCheck) {
			return this.encode();
		}
		if (this.isEmpty(topic) || this.isEmpty(className)) {
			return null;
		} else {
			return this.encode();
		}
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public int getPloy() {
		return ploy;
	}

	public void setPloy(int ploy) {
		this.ploy = ploy;
	}

	public int getInitValue() {
		return initValue;
	}

	public void setInitValue(int initValue) {
		this.initValue = initValue;
	}

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	private boolean isEmpty(String str) {
		if (str == null || str.equals("")) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return "ComponentData [topic=" + topic + ", ploy=" + ploy + ", initValue=" + initValue + ", className="
				+ className + "]";
	}

}
