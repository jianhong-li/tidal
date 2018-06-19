package tidal.store.impl;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Component {

	public static final int COMPONENT_HEADER_SIZE = 36;

	public static int createTimestampHeader = 0;
	public static int updateTimestampHeader = 8;
	public static int ployHeader = 16;
	public static int initValueHeader = 20;
	public static int currentValueHeader = 24;
	public static int topicLengthHeader = 28;
	public static int classNameLengthHeader = 32;

	private long createTime;
	private AtomicLong updateTime = new AtomicLong(0);
	private int ploy;
	private int initValue;
	private AtomicInteger currentValue = new AtomicInteger(0);
	private int topicLength;
	private int classNameLength;
	private String topic;
	private String className;

	private final int offset;
	private final Lock lock = new ReentrantLock();

	public Component(int offset) {
		this.offset = offset;
	}

	public long getCreateTime() {
		return createTime;
	}

	public void setCreateTime(long createTime) {
		this.createTime = createTime;
	}

	public long getUpdateTime() {
		return updateTime.get();
	}

	public void setUpdateTime(long updateTime) {
		this.updateTime.set(updateTime);
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

	public int getCurrentValue() {
		return currentValue.get();
	}

	public void setCurrentValue(int currentValue) {
		this.currentValue.set(currentValue);
	}

	public int getTopicLength() {
		return topicLength;
	}

	public void setTopicLength(int topicLength) {
		this.topicLength = topicLength;
	}

	public int getClassNameLength() {
		return classNameLength;
	}

	public void setClassNameLength(int classNameLength) {
		this.classNameLength = classNameLength;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public int getOffset() {
		return offset;
	}

	public Lock getLock() {
		return lock;
	}

	public int incrCurrentValue() {
		return currentValue.incrementAndGet();
	}

}
