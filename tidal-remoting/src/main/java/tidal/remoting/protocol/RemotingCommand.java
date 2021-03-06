package tidal.remoting.protocol;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.annotation.JSONField;

import tidal.remoting.CommandCustomHeader;
import tidal.remoting.annotation.CFNotNull;
import tidal.remoting.common.LoggerName;
import tidal.remoting.exception.RemotingCommandException;

public class RemotingCommand {
	private static final Logger log = LoggerFactory.getLogger(LoggerName.REMOTING_LOGGER_NAME);

	public static final String REMOTING_VERSION_KEY = "tidal.remoting.version";

	private static final int RPC_TYPE = 0; // 0, REQUEST_COMMAND
	private static final int RPC_ONEWAY = 1; // 0, RPC
	private static final Map<Class<? extends CommandCustomHeader>, Field[]> CLASS_HASH_MAP = new HashMap<Class<? extends CommandCustomHeader>, Field[]>();
	private static final Map<Class, String> CANONICAL_NAME_CACHE = new HashMap<Class, String>();
	private static final Map<Field, Annotation> NOT_NULL_ANNOTATION_CACHE = new HashMap<Field, Annotation>();
	private static final String STRING_CANONICAL_NAME = String.class.getCanonicalName();
	private static final String DOUBLE_CANONICAL_NAME_1 = Double.class.getCanonicalName();
	private static final String DOUBLE_CANONICAL_NAME_2 = double.class.getCanonicalName();
	private static final String INTEGER_CANONICAL_NAME_1 = Integer.class.getCanonicalName();
	private static final String INTEGER_CANONICAL_NAME_2 = int.class.getCanonicalName();
	private static final String LONG_CANONICAL_NAME_1 = Long.class.getCanonicalName();
	private static final String LONG_CANONICAL_NAME_2 = long.class.getCanonicalName();
	private static final String BOOLEAN_CANONICAL_NAME_1 = Boolean.class.getCanonicalName();
	private static final String BOOLEAN_CANONICAL_NAME_2 = boolean.class.getCanonicalName();
	private static volatile int configVersion = -1;
	private static AtomicInteger requestId = new AtomicInteger(0);

	private int code;
	private int version = 0;
	private int opaque = requestId.getAndIncrement();
	private int flag = 0;
	private String remark;
	private HashMap<String, String> extFields;
	private transient CommandCustomHeader customHeader;
	private transient byte[] body;

	public static RemotingCommand createRequestCommand(int code, CommandCustomHeader customHeader) {
		RemotingCommand cmd = new RemotingCommand();
		cmd.setCode(code);
		cmd.customHeader = customHeader;
		setCmdVersion(cmd);
		return cmd;
	}

	private static void setCmdVersion(RemotingCommand cmd) {
		if (configVersion >= 0) {
			cmd.setVersion(configVersion);
		} else {
			String v = System.getProperty(REMOTING_VERSION_KEY);
			if (v != null) {
				int value = Integer.parseInt(v);
				cmd.setVersion(value);
				configVersion = value;
			}
		}
	}

	public static RemotingCommand createResponseCommand(Class<? extends CommandCustomHeader> classHeader) {
		return createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, "not set any response code", classHeader);
	}

	public static RemotingCommand createResponseCommand(int code, String remark,
			Class<? extends CommandCustomHeader> classHeader) {
		RemotingCommand cmd = new RemotingCommand();
		cmd.markResponseType();
		cmd.setCode(code);
		cmd.setRemark(remark);
		setCmdVersion(cmd);

		if (classHeader != null) {
			try {
				CommandCustomHeader objectHeader = classHeader.newInstance();
				cmd.customHeader = objectHeader;
			} catch (InstantiationException e) {
				return null;
			} catch (IllegalAccessException e) {
				return null;
			}
		}

		return cmd;
	}

	public static RemotingCommand createResponseCommand(int code, String remark) {
		return createResponseCommand(code, remark, null);
	}

	public static RemotingCommand decode(final byte[] array) {
		ByteBuffer byteBuffer = ByteBuffer.wrap(array);
		return decode(byteBuffer);
	}

	/**
	 * encode:
	 * +------+---------------+---------------+-------------+
	 * | Salt | Header Length | Actual Header | Actual Body |
	 * | 0x04 |      0x04     |       ?       |      ?      |
	 * +------+-------------------------------+-------------+
	 * 
	 * LengthFieldBasedFrameDecoder(*, 0, 4, 0, 4)
	 * 
	 * decode:
	 * +---------------+---------------+-------------+
	 * | Header Length | Actual Header | Actual Body |
	 * |      0x04     |       ?       |      ?      |
	 * +-------------------------------+-------------+
	 */
	public static RemotingCommand decode(final ByteBuffer byteBuffer) {
		int length = byteBuffer.limit();
		int oriHeaderLen = byteBuffer.getInt();
		int headerLength = getHeaderLength(oriHeaderLen);

		byte[] headerData = new byte[headerLength];
		byteBuffer.get(headerData);

		RemotingCommand cmd = headerDecode(headerData);

		int bodyLength = length - 4 - headerLength;
		byte[] bodyData = null;
		if (bodyLength > 0) {
			bodyData = new byte[bodyLength];
			byteBuffer.get(bodyData);
		}
		cmd.body = bodyData;

		return cmd;
	}

	private static RemotingCommand headerDecode(byte[] headerData) {
		return RemotingSerializable.decode(headerData, RemotingCommand.class);
	}

	public static int getHeaderLength(int length) {
		return length & 0xFFFFFF;
	}

	/**
	 * +------+---------------+---------------+-------------+
	 * | Salt | Header Length | Actual Header | Actual Body |
	 * | 0x04 |      0x04     |       ?       |      ?      |
	 * +------+-------------------------------+-------------+
	 */
	public ByteBuffer encode() {
		// 1> header length size(salt)
		int length = 4;

		// 2> header data length
		byte[] headerData = this.headerEncode();
		length += headerData.length;

		// 3> body data length
		if (this.body != null) {
			length += body.length;
		}

		ByteBuffer result = ByteBuffer.allocate(4 + length);

		// length
		result.putInt(length);

		// header length
		result.putInt(headerData.length);

		// header data
		result.put(headerData);

		// body data;
		if (this.body != null) {
			result.put(this.body);
		}

		result.flip();

		return result;
	}

	public ByteBuffer encodeHeader() {
		return encodeHeader(this.body != null ? this.body.length : 0);
	}

	/**
	 * +------+---------------+---------------+
	 * | Salt | Header Length | Actual Header |
	 * | 0x04 |      0x04     |       ?       |
	 * +------+-------------------------------+
	 */
	public ByteBuffer encodeHeader(final int bodyLength) {
		// 1> header length size(salt)
		int length = 4;

		// 2> header data length
		byte[] headerData;
		headerData = this.headerEncode();

		length += headerData.length;

		// 3> body data length
		length += bodyLength;

		ByteBuffer result = ByteBuffer.allocate(4 + length - bodyLength);

		// length
		result.putInt(length);

		// header length
		result.putInt(headerData.length);

		// header data
		result.put(headerData);

		result.flip();

		return result;
	}

	private byte[] headerEncode() {
		this.makeCustomHeaderToNet();
		return RemotingSerializable.encode(this);
	}

	public void makeCustomHeaderToNet() {
		if (this.customHeader != null) {
			Field[] fields = getClazzFields(customHeader.getClass());
			if (null == this.extFields) {
				this.extFields = new HashMap<String, String>();
			}

			for (Field field : fields) {
				if (!Modifier.isStatic(field.getModifiers())) {
					String name = field.getName();
					if (!name.startsWith("this")) {
						Object value = null;
						try {
							field.setAccessible(true);
							value = field.get(this.customHeader);
						} catch (Exception e) {
							log.error("Failed to access field [{}]", name, e);
						}

						if (value != null) {
							this.extFields.put(name, value.toString());
						}
					}
				}
			}
		}
	}

	public CommandCustomHeader decodeCommandCustomHeader(Class<? extends CommandCustomHeader> classHeader)
			throws RemotingCommandException {
		CommandCustomHeader objectHeader;
		try {
			objectHeader = classHeader.newInstance();
		} catch (InstantiationException e) {
			return null;
		} catch (IllegalAccessException e) {
			return null;
		}

		if (this.extFields != null) {

			Field[] fields = getClazzFields(classHeader);
			for (Field field : fields) {
				if (!Modifier.isStatic(field.getModifiers())) {
					String fieldName = field.getName();
					if (!fieldName.startsWith("this")) {
						try {
							String value = this.extFields.get(fieldName);
							if (null == value) {
								Annotation annotation = getNotNullAnnotation(field);
								if (annotation != null) {
									throw new RemotingCommandException("the custom field <" + fieldName + "> is null");
								}

								continue;
							}

							field.setAccessible(true);
							String type = getCanonicalName(field.getType());
							Object valueParsed;

							if (type.equals(STRING_CANONICAL_NAME)) {
								valueParsed = value;
							} else if (type.equals(INTEGER_CANONICAL_NAME_1) || type.equals(INTEGER_CANONICAL_NAME_2)) {
								valueParsed = Integer.parseInt(value);
							} else if (type.equals(LONG_CANONICAL_NAME_1) || type.equals(LONG_CANONICAL_NAME_2)) {
								valueParsed = Long.parseLong(value);
							} else if (type.equals(BOOLEAN_CANONICAL_NAME_1) || type.equals(BOOLEAN_CANONICAL_NAME_2)) {
								valueParsed = Boolean.parseBoolean(value);
							} else if (type.equals(DOUBLE_CANONICAL_NAME_1) || type.equals(DOUBLE_CANONICAL_NAME_2)) {
								valueParsed = Double.parseDouble(value);
							} else {
								throw new RemotingCommandException(
										"the custom field <" + fieldName + "> type is not supported");
							}

							field.set(objectHeader, valueParsed);

						} catch (Throwable e) {
							log.error("Failed field [{}] decoding", fieldName, e);
						}
					}
				}
			}

			objectHeader.checkFields();
		}

		return objectHeader;
	}

	private Field[] getClazzFields(Class<? extends CommandCustomHeader> classHeader) {
		Field[] field = CLASS_HASH_MAP.get(classHeader);

		if (field == null) {
			field = classHeader.getDeclaredFields();
			synchronized (CLASS_HASH_MAP) {
				CLASS_HASH_MAP.put(classHeader, field);
			}
		}
		return field;
	}

	private Annotation getNotNullAnnotation(Field field) {
		Annotation annotation = NOT_NULL_ANNOTATION_CACHE.get(field);

		if (annotation == null) {
			annotation = field.getAnnotation(CFNotNull.class);
			synchronized (NOT_NULL_ANNOTATION_CACHE) {
				NOT_NULL_ANNOTATION_CACHE.put(field, annotation);
			}
		}
		return annotation;
	}

	private String getCanonicalName(Class clazz) {
		String name = CANONICAL_NAME_CACHE.get(clazz);

		if (name == null) {
			name = clazz.getCanonicalName();
			synchronized (CANONICAL_NAME_CACHE) {
				CANONICAL_NAME_CACHE.put(clazz, name);
			}
		}
		return name;
	}

	public static Object parseForm(final ByteBuffer byteBuffer) {
		byte version = byteBuffer.get();

		switch (version) {
		case 1:
			return parseBody(byteBuffer);
		default:
			return parseBody(byteBuffer);
		}
	}

	public static RemotingCommand parseBody(final ByteBuffer byteBuffer) {
		short bodyLength = byteBuffer.getShort();

		byte[] bodyData = null;
		if (bodyLength > 0) {
			bodyData = new byte[bodyLength];
			byteBuffer.get(bodyData);
		}

		RemotingCommand cmd = RemotingSerializable.decode(bodyData, RemotingCommand.class);
		cmd.body = bodyData;
		return cmd;
	}

	public CommandCustomHeader readCustomHeader() {
		return customHeader;
	}

	public void writeCustomHeader(CommandCustomHeader customHeader) {
		this.customHeader = customHeader;
	}

	public static AtomicInteger getRequestId() {
		return requestId;
	}

	public static void setRequestId(AtomicInteger requestId) {
		RemotingCommand.requestId = requestId;
	}

	@JSONField(serialize = false)
	public RemotingCommandType getType() {
		if (this.isResponseType()) {
			return RemotingCommandType.RESPONSE_COMMAND;
		}

		return RemotingCommandType.REQUEST_COMMAND;
	}

	public void markResponseType() {
		int bits = 1 << RPC_TYPE;
		this.flag |= bits;
	}

	@JSONField(serialize = false)
	public boolean isResponseType() {
		int bits = 1 << RPC_TYPE;
		return (this.flag & bits) == bits;
	}

	public void markOnewayRPC() {
		int bits = 1 << RPC_ONEWAY;
		this.flag |= bits;
	}

	@JSONField(serialize = false)
	public boolean isOnewayRPC() {
		int bits = 1 << RPC_ONEWAY;
		return (this.flag & bits) == bits;
	}

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public byte[] getBody() {
		return body;
	}

	public void setBody(byte[] body) {
		this.body = body;
	}

	public int getOpaque() {
		return opaque;
	}

	public void setOpaque(int opaque) {
		this.opaque = opaque;
	}

	public int getFlag() {
		return flag;
	}

	public void setFlag(int flag) {
		this.flag = flag;
	}

	public String getRemark() {
		return remark;
	}

	public void setRemark(String remark) {
		this.remark = remark;
	}

	public HashMap<String, String> getExtFields() {
		return extFields;
	}

	public void setExtFields(HashMap<String, String> extFields) {
		this.extFields = extFields;
	}

	public void addExtField(String key, String value) {
		if (null == extFields) {
			extFields = new HashMap<String, String>();
		}
		extFields.put(key, value);
	}

	@Override
	public String toString() {
		return "RemotingCommand [code=" + code + ", version=" + version + ", opaque=" + opaque + ", flag(B)="
				+ Integer.toBinaryString(flag) + ", remark=" + remark + ", extFields=" + extFields + "]";
	}

}
