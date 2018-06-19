package tidal.store.impl;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tidal.store.MappedFile;
import tidal.store.common.LoggerName;
import tidal.store.config.ModeMapping;
import tidal.store.config.StoreConfig;
import tidal.store.config.StorePathConfigHelper;
import tidal.store.logic.LogicClassLoader;
import tidal.store.logic.LogicImpl;

public class PhysicalFile {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

	private static final int FILE_SLICE_SIZE = 1024 * 1024 * 8;

	private File file;
	private final MappedFile mappedFile;
	private final MappedByteBuffer mappedByteBuffer;
	private final ByteBuffer haSliceByteBuffer;
	private final StoreHeader storeHeader;
	private final StoreComponent storeComponent;
	private final LogicImpl logicImpl;

	public PhysicalFile(final StoreConfig storeConfig) throws IOException {
		String storeFileName = StorePathConfigHelper.getStoreFilePath(storeConfig.getStoreRootDir()) + File.separator
				+ StoreConfig.TIDAL_STORE_FILE_NAME;

		boolean isfirst = this.isFirst(storeFileName);
		if (isfirst) {
			this.mappedFile = new MappedFile(storeFileName, FILE_SLICE_SIZE);
		} else {
			this.mappedFile = new MappedFile(file, FILE_SLICE_SIZE);
		}

		this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
		this.haSliceByteBuffer = mappedByteBuffer.slice();

		this.mappedByteBuffer.limit(StoreHeader.STORE_HEADER_SIZE);
		ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
		this.storeHeader = new StoreHeader(byteBuffer);

		if (isfirst) {
			this.storeHeader.write(ModeMapping.getMap(storeConfig.isCluster()), 0, 0);
		} else {
			this.storeHeader.write(ModeMapping.getMap(storeConfig.isCluster()));
			this.storeHeader.load();
		}

		this.mappedByteBuffer.clear();
		this.mappedByteBuffer.position(StoreHeader.STORE_HEADER_SIZE);

		this.storeComponent = new StoreComponent(mappedByteBuffer);
		this.logicImpl = new LogicImpl();
	}

	public void load() {
		if (this.storeHeader.getLastComponentOffset() == 0) {
			return;
		}

		int position = this.mappedByteBuffer.position();
		if (position < this.storeHeader.getLastComponentOffset()) {
			getNewComponent(this.mappedByteBuffer);
			this.load();
		} else if (position == this.storeHeader.getLastComponentOffset()) {
			getNewComponent(this.mappedByteBuffer);
		} else {
			log.warn("tidal store IndexOutOfBoundsException: " + "mappedByteBuffer position=" + position
					+ ", lastOffset=" + this.storeHeader.getLastComponentOffset());
			System.exit(1);
		}
	}

	private void getNewComponent(final MappedByteBuffer mappedByteBuffer) {
		Component component = new Component(mappedByteBuffer.position());

		try {
			storeComponent.load(component);
			if (storeComponent.checkInitValue(component.getInitValue())
					&& LogicClassLoader.isPresent(component.getClassName())) {
				logicImpl.add(component);
				storeComponent.addMap(component);
				log.info(
						"add cache success: topic = " + component.getTopic() + " className: " + component.getClassName());
			} else {
				log.warn("add cache failure, cause: initValue = " + component.getInitValue() + " or class '"
						+ component.getClassName() + "' not found, skip...");
			}
		} catch (UnsupportedEncodingException e) {
			log.error("newContentUnit error: ", e.getMessage());
		}
	}

	private boolean isFirst(String fileName) {
		this.file = new File(fileName);
		if (file.exists()) {
			return false;
		} else {
			return true;
		}
	}

	public void save() {
		this.mappedFile.flush();
	}

	public void destroy() {
		this.mappedFile.destroy();
	}

	public boolean isFull(int size) {
		if (mappedByteBuffer.capacity() - mappedByteBuffer.position() >= size) {
			return false;
		} else {
			return true;
		}
	}

	public MappedByteBuffer getMappedByteBuffer() {
		return mappedByteBuffer;
	}

	public ByteBuffer getHaSliceByteBuffer() {
		return haSliceByteBuffer;
	}

	public StoreHeader getStoreHeader() {
		return storeHeader;
	}

	public StoreComponent getStoreComponent() {
		return storeComponent;
	}

	public LogicImpl getLogicImpl() {
		return logicImpl;
	}

}
