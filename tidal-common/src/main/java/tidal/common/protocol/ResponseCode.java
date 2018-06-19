package tidal.common.protocol;

import tidal.remoting.protocol.RemotingSysResponseCode;

public class ResponseCode extends RemotingSysResponseCode {

	public static final int TOPIC_EXISTED = 10;

	public static final int TOPIC_NOT_EXISTED = 11;

	public static final int INIT_VALUE_INVALID = 12;

	public static final int CLASS_NOT_FOUND = 13;

	public static final int FLUSH_DISK_FULL = 14;

	public static final int PASSWORD_ERROR = 15;

	public static final int PLOY_NOT_SUPPORTED = 16;
	
	public static final int PLOY_CLASS_MAPPING_ERROR = 17;
}
