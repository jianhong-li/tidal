package tidal.remoting.protocol;

public enum ProtocolVersion {

	V_1_0((byte) 1);

	private byte version;

	ProtocolVersion(byte version) {
		this.version = version;
	}

	public byte get() {
		return version;
	}

	public static byte getLatest() {
		ProtocolVersion[] values = ProtocolVersion.values();
		return values[values.length - 1].version;
	}
}
