package tidal.store.impl;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class StoreHeader {

	public static final int STORE_HEADER_SIZE = 32;
	private static int beginTimestampHeader = 0;
	private static int endTimestampHeader = 8;
	private static int runModeHeader = 16;
	private static int mySidHeader = 20;
	private static int brotherSidHeader = 24;
	public static int lastComponentOffsetHeader = 28;

	private final ByteBuffer byteBuffer;
	private AtomicLong beginTimestamp = new AtomicLong(0);
	private AtomicLong endTimestamp = new AtomicLong(0);

	private AtomicInteger runMode = new AtomicInteger(0);
	private AtomicInteger mySid = new AtomicInteger(0);
	private AtomicInteger brotherSid = new AtomicInteger(0);

	private AtomicInteger lastComponentOffset = new AtomicInteger(0);

	public StoreHeader(final ByteBuffer byteBuffer) {
		this.byteBuffer = byteBuffer;
	}

	/**
	 * init
	 * @param runMode
	 * @param mySid
	 * @param brotherSid
	 */
	public void write(int runMode, int mySid, int brotherSid) {
		this.write(runMode);

		this.setMySid(mySid);
		this.setBrotherSid(brotherSid);
		this.setLastComponentOffset(0);
	}

	public void write(int runMode) {
		long thisTime = new Date().getTime();

		this.setBeginTimestamp(thisTime);
		this.setEndTimestamp(thisTime);
		if (runMode == 1 && byteBuffer.getInt(runModeHeader) == 0) {
			this.setRunMode(1);
		} else {
			this.setRunMode(runMode);
		}

	}

	public void load() {
		this.beginTimestamp.set(byteBuffer.getLong(beginTimestampHeader));
		this.endTimestamp.set(byteBuffer.getLong(endTimestampHeader));
		this.runMode.set(byteBuffer.getInt(runModeHeader));
		this.mySid.set(byteBuffer.getInt(mySidHeader));
		this.brotherSid.set(byteBuffer.getInt(brotherSidHeader));
		this.lastComponentOffset.set(byteBuffer.getInt(lastComponentOffsetHeader));
	}

	public void updateByteBuffer() {
		this.byteBuffer.putLong(beginTimestampHeader, this.beginTimestamp.get());
		this.byteBuffer.putLong(endTimestampHeader, this.endTimestamp.get());
		this.byteBuffer.putInt(runModeHeader, this.runMode.get());
		this.byteBuffer.putInt(mySidHeader, this.mySid.get());
		this.byteBuffer.putInt(brotherSidHeader, this.brotherSid.get());
		this.byteBuffer.putInt(lastComponentOffsetHeader, this.lastComponentOffset.get());
	}

	public long getBeginTimestamp() {
		return beginTimestamp.get();
	}

	public void setBeginTimestamp(long beginTimestamp) {
		this.beginTimestamp.set(beginTimestamp);
		this.byteBuffer.putLong(beginTimestampHeader, beginTimestamp);
	}

	public long getEndTimestamp() {
		return endTimestamp.get();
	}

	public void setEndTimestamp(long endTimestamp) {
		this.endTimestamp.set(endTimestamp);
		this.byteBuffer.putLong(endTimestampHeader, endTimestamp);
	}

	public int getRunMode() {
		return runMode.get();
	}

	public void setRunMode(int runMode) {
		this.runMode.set(runMode);
		this.byteBuffer.putInt(runModeHeader, runMode);
	}

	public int getMySid() {
		return mySid.get();
	}

	public void setMySid(int mySid) {
		this.mySid.set(mySid);
		this.byteBuffer.putInt(mySidHeader, mySid);
	}

	public int getBrotherSid() {
		return brotherSid.get();
	}

	public void setBrotherSid(int brotherSid) {
		this.brotherSid.set(brotherSid);
		this.byteBuffer.putInt(brotherSidHeader, brotherSid);
	}

	public int getLastComponentOffset() {
		return lastComponentOffset.get();
	}

	public void setLastComponentOffset(int lastComponentOffset) {
		this.lastComponentOffset.set(lastComponentOffset);
		this.byteBuffer.putInt(lastComponentOffsetHeader, lastComponentOffset);
	}

}
