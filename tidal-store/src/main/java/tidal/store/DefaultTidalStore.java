package tidal.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tidal.common.SystemClock;
import tidal.common.protocol.body.ComponentData;
import tidal.store.common.LoggerName;
import tidal.store.config.StoreConfig;
import tidal.store.config.StorePathConfigHelper;
import tidal.store.ha.HAService;
import tidal.store.impl.StoreService;

public class DefaultTidalStore implements TidalStore {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

	private volatile boolean shutdown = true;

	private final StoreService storeService;
	private final HAService haService;
	private final StoreConfig storeConfig;
	private RandomAccessFile lockFile;

	private FileLock lock;

	private final SystemClock systemClock = new SystemClock();

	public DefaultTidalStore(final StoreConfig storeConfig) throws IOException {
		this.storeConfig = storeConfig;

		this.storeService = new StoreService(this);
		this.haService = new HAService(this);

		File file = new File(StorePathConfigHelper.getLockFile(storeConfig.getStoreRootDir()));
		MappedFile.ensureDirOK(file.getParent());
		lockFile = new RandomAccessFile(file, "rw");
	}

	@Override
	public void start() throws Exception {
		lock = lockFile.getChannel().tryLock(0, 1, false);

		if (lock == null || lock.isShared() || !lock.isValid()) {
			throw new RuntimeException("Lock failed, Tidal already started");
		}

		lockFile.getChannel().write(ByteBuffer.wrap("lock".getBytes()));
		lockFile.getChannel().force(true);

		this.storeService.start();

		if (storeConfig.isCluster()) {
			this.haService.start();
		}

		this.shutdown = false;
	}

	@Override
	public void shutdown() {
		if (!this.shutdown) {
			this.shutdown = true;

			if (storeConfig.isCluster()) {
				this.haService.shutdown();
			}

			this.storeService.shutdown();
		}
		if (lockFile != null && lock != null) {
			try {
				lock.release();
				lockFile.close();
			} catch (IOException e) {
				log.error("tidal file close fail: ", e.getMessage());
			}
		}
	}

	@Override
	public void updateHaMasterAddress(String newAddr) {
		haService.updateMasterAddress(newAddr);
	}

	@Override
	public List<ComponentData> queryAllTopic() {
		return storeService.queryAllTopic();
	}

	@Override
	public int putComponent(String reqId, int ploy, int initValue, String className) {
		return storeService.putComponent(reqId, ploy, initValue, className);
	}

	@Override
	public String updateComponent(String reqId) {
		return storeService.updateComponent(reqId);
	}

	@Override
	public boolean isOSPageCacheBusy() {
		long diff = this.systemClock.now() - storeService.getBeginTimeInLock();
		if (diff < 10000000 //
				&& diff > this.storeConfig.getOsPageCacheBusyTimeOutMills()) {
			return true;
		}

		return false;
	}

	public SystemClock getSystemClock() {
		return systemClock;
	}

	public StoreConfig getStoreConfig() {
		return storeConfig;
	}

	public boolean isShutdown() {
		return shutdown;
	}

	public StoreService getStoreService() {
		return storeService;
	}

	public HAService getHaService() {
		return haService;
	}

}
