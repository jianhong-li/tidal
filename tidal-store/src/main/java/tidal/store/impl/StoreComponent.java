package tidal.store.impl;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import tidal.store.config.StoreConfig;

public class StoreComponent {

	/**
	 * final static filed: ploy type
	 */
	public final static int INCREMENT_FIELD = 0;
	public final static int YEAR_FIELD = 1;
	public final static int MONTH_FIELD = 2;
	public final static int DAY_FIELD = 3;

	private final ConcurrentMap<String /**topic*/
			, Component> componentMap = new ConcurrentHashMap<String, Component>();

	private final MappedByteBuffer mappedByteBuffer;
	private int lastOffset;

	public StoreComponent(final MappedByteBuffer mappedByteBuffer) {
		this.mappedByteBuffer = mappedByteBuffer;
		this.lastOffset = mappedByteBuffer.position();
	}

	public void load(final Component component) throws UnsupportedEncodingException {
		long createTimestamp = mappedByteBuffer.getLong();
		long updateTimestamp = mappedByteBuffer.getLong();
		int ploy = mappedByteBuffer.getInt();
		int initValue = mappedByteBuffer.getInt();
		int currentValue = mappedByteBuffer.getInt();
		int topicLength = mappedByteBuffer.getInt();
		int classNameLength = mappedByteBuffer.getInt();

		component.setCreateTime(createTimestamp);
		component.setUpdateTime(updateTimestamp);
		component.setPloy(ploy);
		component.setInitValue(initValue);
		component.setCurrentValue(currentValue);
		component.setTopicLength(topicLength);
		component.setClassNameLength(classNameLength);

		byte topic[] = new byte[topicLength];
		this.get(topic);
		component.setTopic(new String(topic, StoreConfig.TIDAL_STORE_DEFAULT_CHARSET));
		byte className[] = new byte[classNameLength];
		this.get(className);
		component.setClassName(new String(className, StoreConfig.TIDAL_STORE_DEFAULT_CHARSET));

		this.lastOffset = mappedByteBuffer.position();
	}

	public void write(final Component component)
			throws IllegalArgumentException, IllegalAccessException, UnsupportedEncodingException {
		Class<? extends Component> cls = component.getClass();
		Field[] declaredFields = cls.getDeclaredFields();
		for (Field field : declaredFields) {

			if (!Modifier.isStatic(field.getModifiers()) && !Modifier.isFinal(field.getModifiers())) {
				field.setAccessible(true);
				String value = field.get(component).toString();

				String fieldTypeName = field.getType().getSimpleName();
				if (fieldTypeName.equals("String")) {
					put(value.getBytes(StoreConfig.TIDAL_STORE_DEFAULT_CHARSET));
				} else if (fieldTypeName.equals("int") || fieldTypeName.equals("AtomicInteger")) {
					putInt(Integer.parseInt(value));
				} else if (fieldTypeName.equals("long") || fieldTypeName.equals("AtomicLong")) {
					putLong(Long.parseLong(value));
				}
			}
		}

		this.lastOffset = mappedByteBuffer.position();
	}

	public void update(final Component component) {
		mappedByteBuffer.putLong(component.getOffset() + Component.updateTimestampHeader, component.getUpdateTime());
		mappedByteBuffer.putInt(component.getOffset() + Component.currentValueHeader, component.getCurrentValue());
	}

	public void reset(final Component component) {
		component.setCurrentValue(component.getInitValue());
		mappedByteBuffer.putInt(component.getOffset() + Component.currentValueHeader, component.getInitValue());
	}

	public void addMap(final Component component) {
		componentMap.put(component.getTopic(), component);
	}

	public boolean isExistTopic(String key) {
		return componentMap.containsKey(key);
	}

	public boolean checkInitValue(int initValue) {
		if (initValue >= 0) {
			return true;
		} else {
			return false;
		}
	}

	public boolean isExistPloyType(int ploy) {
		Field[] declaredFields = StoreComponent.class.getDeclaredFields();

		boolean flag = false;
		for (Field field : declaredFields) {
			if (Modifier.isStatic(field.getModifiers()) && Modifier.isFinal(field.getModifiers())
					&& field.getType().getSimpleName().equals("int")) {
				field.setAccessible(true);

				Integer already = 0;
				try {
					already = (Integer) field.get(StoreComponent.class);
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				}
				if (ploy == already) {
					flag = true;
					break;
				}
			}
		}
		return flag;
	}

	private void put(byte[] src) {
		for (int i = 0; i < src.length; i++) {
			mappedByteBuffer.put(src[i]);
		}
	}

	private byte[] get(byte[] dest) {
		for (int i = 0; i < dest.length; i++) {
			dest[i] = mappedByteBuffer.get();
		}
		return dest;
	}

	public static void put(ByteBuffer byteBuffer, int offset, byte[] src) {
		for (int i = 0; i < src.length; i++) {
			byteBuffer.put(offset + i, src[i]);
		}
	}

	public static byte[] get(ByteBuffer byteBuffer, int offset, byte[] dest) {
		for (int i = 0; i < dest.length; i++) {
			dest[i] = byteBuffer.get(offset + i);
		}
		return dest;
	}

	private static int getCurrentYear() {
		return LocalDateTime.now().getYear();
	}

	private static int getCurrentMonth() {
		return LocalDateTime.now().getMonth().getValue();
	}

	private static int getCurrentDay() {
		return LocalDateTime.now().getDayOfMonth();
	}

	private static LocalDateTime time2Date(long time) {
		return Instant.ofEpochMilli(time).atZone(ZoneId.of(ZoneId.systemDefault().getId())).toLocalDateTime();
	}

	public boolean isReset(final Component component) {
		if (component.getPloy() == INCREMENT_FIELD) {
			return false;
		} else if (component.getPloy() == YEAR_FIELD) {
			int year = time2Date(component.getUpdateTime()).getYear();
			if (getCurrentYear() == year) {
				return false;
			} else {
				return true;
			}
		} else if (component.getPloy() == MONTH_FIELD) {
			int month = time2Date(component.getUpdateTime()).getMonth().getValue();
			if (getCurrentMonth() == month) {
				return false;
			} else {
				return true;
			}
		} else if (component.getPloy() == DAY_FIELD) {
			int dayOfMonth = time2Date(component.getUpdateTime()).getDayOfMonth();
			if (getCurrentDay() == dayOfMonth) {
				return false;
			} else {
				return true;
			}
		} else {
			return false;
		}
	}

	public void setLastOffset(int remotingLength) {
		this.lastOffset += remotingLength;
	}

	public int getLastOffset() {
		return lastOffset;
	}

	private void putInt(int dest) {
		mappedByteBuffer.putInt(dest);
	}

	private void putLong(long dest) {
		mappedByteBuffer.putLong(dest);
	}

	public boolean componentMapContainsKey(String key) {
		return componentMap.containsKey(key);
	}

	public ConcurrentMap<String, Component> getComponentMap() {
		return componentMap;
	}

}
