package tidal.store.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tidal.common.ServiceThread;
import tidal.remoting.common.RemotingHelper;
import tidal.remoting.common.RemotingUtil;
import tidal.store.DefaultTidalStore;
import tidal.store.common.LoggerName;
import tidal.store.impl.StoreComponent;
import tidal.store.impl.StoreHeader;

public class HAService {
	private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

	private final AtomicInteger connectionCount = new AtomicInteger(0);

	private final List<HAConnection> connectionList = new LinkedList<>();

	private final AcceptSocketService acceptSocketService;

	private final DefaultTidalStore defaultTidalStore;

	private final WaitNotifyObject waitNotifyObject = new WaitNotifyObject();

	private final HandlerService handlerService;

	private final HAClient haClient;

	private long lastStartHAConnectionTime = -1;
	private long remotingStartHAConnectionTime = -1;
	private int remotingMySid = -1;
	private int remotingBrotherSid = -1;
	private int remotingLastOffset = -1;

	public HAService(final DefaultTidalStore defaultTidalStore) throws IOException {
		this.defaultTidalStore = defaultTidalStore;
		this.acceptSocketService = new AcceptSocketService(defaultTidalStore.getStoreConfig().getHaListenPort());
		this.handlerService = new HandlerService();
		this.haClient = new HAClient();
	}

	public void updateMasterAddress(final String newAddr) {
		if (this.haClient != null) {
			this.haClient.updateMasterAddress(newAddr);
		}
	}

	public int getRemotingLastOffset() {
		return remotingLastOffset;
	}

	public void setRemotingLastOffset(int remotingLastOffset) {
		this.remotingLastOffset = remotingLastOffset;
	}

	public long getLastStartHAConnectionTime() {
		return lastStartHAConnectionTime;
	}

	public void notifyUpdateSid(final long remotingTime, final int remotingMySid, final int remotingBrotherSid) {
		this.remotingMySid = remotingMySid;
		this.remotingBrotherSid = remotingBrotherSid;
		notifyTransferSome(remotingTime);
	}

	public void notifyTransferSome(final long remotingTime) {
		this.remotingStartHAConnectionTime = remotingTime;
		notifyTransferSome();
	}

	public void notifyTransferSome() {
		for (;;) {
			if (lastStartHAConnectionTime > 0) {
				this.handlerService.notifyHandlerSome();
				break;
			}
		}
	}

	public AtomicInteger getConnectionCount() {
		return connectionCount;
	}

	public boolean start() throws Exception {
		this.acceptSocketService.beginAccept();
		this.acceptSocketService.start();
		this.handlerService.start();
		this.haClient.start();
		waitNotifyObject.waitForRunning(30000);
		return true;
	}

	public void addConnection(final HAConnection conn) {
		synchronized (this.connectionList) {
			this.connectionList.add(conn);
		}
	}

	public void removeConnection(final HAConnection conn) {
		synchronized (this.connectionList) {
			this.connectionList.remove(conn);
		}
	}

	public void shutdown() {
		this.haClient.shutdown();
		this.acceptSocketService.shutdown(true);
		this.destroyConnections();
		this.handlerService.shutdown();
	}

	public void destroyConnections() {
		synchronized (this.connectionList) {
			for (HAConnection c : this.connectionList) {
				c.shutdown();
			}

			this.connectionList.clear();
		}
	}

	public WaitNotifyObject getWaitNotifyObject() {
		return waitNotifyObject;
	}

	public DefaultTidalStore getDefaultTidalStore() {
		return defaultTidalStore;
	}

	/**
	 * Listens to node connections to create {@link HAConnection}.
	 */
	class AcceptSocketService extends ServiceThread {
		private final SocketAddress socketAddressListen;
		private ServerSocketChannel serverSocketChannel;
		private Selector selector;

		public AcceptSocketService(final int port) {
			this.socketAddressListen = new InetSocketAddress(port);
		}

		/**
		 * Starts listening to node connections.
		 *
		 * @throws Exception
		 *             If fails.
		 */
		public void beginAccept() throws Exception {
			this.serverSocketChannel = ServerSocketChannel.open();
			this.selector = RemotingUtil.openSelector();
			this.serverSocketChannel.socket().setReuseAddress(true);
			this.serverSocketChannel.socket().bind(this.socketAddressListen);
			this.serverSocketChannel.configureBlocking(false);
			this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
		}

		/** {@inheritDoc} */
		@Override
		public void shutdown(final boolean interrupt) {
			super.shutdown(interrupt);
			try {
				this.serverSocketChannel.close();
				this.selector.close();
			} catch (IOException e) {
				log.error("AcceptSocketService shutdown exception", e);
			}
		}

		/** {@inheritDoc} */
		@Override
		public void run() {
			log.info(this.getServiceName() + " service started");

			while (!this.isStopped()) {
				try {
					this.selector.select(1000);
					Set<SelectionKey> selected = this.selector.selectedKeys();

					if (selected != null) {
						for (SelectionKey k : selected) {
							if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
								SocketChannel sc = ((ServerSocketChannel) k.channel()).accept();

								if (sc != null) {
									HAService.log.info("HAService receive new connection, "
											+ sc.socket().getRemoteSocketAddress());
									HAService.this.lastStartHAConnectionTime = System.currentTimeMillis();

									try {
										HAConnection conn = new HAConnection(HAService.this, sc);
										conn.start();
										HAService.this.addConnection(conn);
									} catch (Exception e) {
										log.error("new HAConnection exception", e);
										sc.close();
									}
								}
							} else {
								log.warn("Unexpected ops in select " + k.readyOps());
							}
						}

						selected.clear();
					}
				} catch (Exception e) {
					log.error(this.getServiceName() + " service has exception.", e);
				}
			}

			log.info(this.getServiceName() + " service end");
		}

		/** {@inheritDoc} */
		@Override
		public String getServiceName() {
			return AcceptSocketService.class.getSimpleName();
		}
	}

	class HandlerService extends ServiceThread {

		public void notifyHandlerSome() {
			this.wakeup();
		}

		private void doWaitHandle() {
			if (lastStartHAConnectionTime > 0 && remotingStartHAConnectionTime > 1) {
				if (HAService.this.getDefaultTidalStore().getStoreService().getMySid() == 0
						&& HAService.this.getDefaultTidalStore().getStoreService().getBrotherSid() == 0) {
					init();
				}
			} else if (lastStartHAConnectionTime > 0 && remotingStartHAConnectionTime == 1) {
				if (HAService.this.getDefaultTidalStore().getStoreService().getMySid() == 0
						|| HAService.this.getDefaultTidalStore().getStoreService().getBrotherSid() == 0) {
					sync(2);
				}
			} else if (lastStartHAConnectionTime > 0 && remotingStartHAConnectionTime == 0) {
				if (HAService.this.getDefaultTidalStore().getStoreService().getMySid() >= 0
						|| HAService.this.getDefaultTidalStore().getStoreService().getBrotherSid() >= 0) {
					sync(0);
				}
			} else {
				// ignore
			}
		}

		private synchronized void init() {
			if (lastStartHAConnectionTime < remotingStartHAConnectionTime) {
				HAService.this.getDefaultTidalStore().getStoreService().setMySid(1);
				HAService.this.getDefaultTidalStore().getStoreService().setBrotherSid(2);
				log.debug(this.getServiceName() + " init sid: self 1, brother 2");
				HAService.this.waitNotifyObject.wakeup();
			} else if (lastStartHAConnectionTime > remotingStartHAConnectionTime) {
				log.debug(this.getServiceName() + " init sid: self 2, brother 1");
				HAService.this.getDefaultTidalStore().getStoreService().setMySid(2);
				HAService.this.getDefaultTidalStore().getStoreService().setBrotherSid(1);
				HAService.this.waitNotifyObject.wakeup();
			} else {
				// ignore
			}
		}

		private synchronized void sync(int step) {
			if (remotingMySid > 0 && remotingBrotherSid > 0
					&& HAService.this.getDefaultTidalStore().getStoreService().getMySid() == 0
					&& HAService.this.getDefaultTidalStore().getStoreService().getBrotherSid() == 0) {
				int mySelfSid = remotingBrotherSid + step;
				HAService.this.getDefaultTidalStore().getStoreService().setMySid(mySelfSid);
				HAService.this.getDefaultTidalStore().getStoreService().setBrotherSid(remotingMySid);

				log.debug(
						this.getServiceName() + " sync sid success: self " + mySelfSid + ", brother " + remotingMySid);
				HAService.this.waitNotifyObject.wakeup();
			} else if (remotingMySid > 0 && remotingBrotherSid > 0
					&& HAService.this.getDefaultTidalStore().getStoreService().getMySid() > 0
					&& HAService.this.getDefaultTidalStore().getStoreService().getBrotherSid() > 0) {
				if (remotingMySid > HAService.this.getDefaultTidalStore().getStoreService().getBrotherSid()) {
					HAService.this.getDefaultTidalStore().getStoreService().setBrotherSid(remotingMySid);

					log.debug(this.getServiceName() + " sync sid success: brother " + remotingMySid);
					HAService.this.waitNotifyObject.wakeup();
				}
			}
		}

		@Override
		public void run() {
			while (!this.isStopped()) {
				try {
					this.waitForRunning(500);
					this.doWaitHandle();
				} catch (Exception e) {
					log.warn(this.getServiceName() + " service has exception. ", e);
				}
			}
		}

		@Override
		public String getServiceName() {
			return HandlerService.class.getSimpleName();
		}

	}

	class HAClient extends ServiceThread {
		private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024 * 8;

		private final AtomicReference<String> masterAddress = new AtomicReference<>();
		private final ByteBuffer reportSid = ByteBuffer.allocate(16);
		private SocketChannel socketChannel;
		private Selector selector;
		private long lastWriteTimestamp = System.currentTimeMillis();

		private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);

		public HAClient() throws IOException {
			this.selector = RemotingUtil.openSelector();
		}

		public void updateMasterAddress(final String newAddr) {
			String currentAddr = this.masterAddress.get();
			if (currentAddr == null || !currentAddr.equals(newAddr)) {
				this.masterAddress.set(newAddr);
				log.info("update master address, OLD: " + currentAddr + " NEW: " + newAddr);
			}
		}

		private boolean isTimeToReportSid() {
			long interval = HAService.this.defaultTidalStore.getSystemClock().now() - this.lastWriteTimestamp;
			boolean needHeart = interval > HAService.this.defaultTidalStore.getStoreConfig()
					.getHaSendHeartbeatInterval();

			return needHeart;
		}

		private boolean reportLatestSid() {
			long header = -1;
			if (HAService.this.defaultTidalStore.getStoreService().getLastOffset() == StoreHeader.STORE_HEADER_SIZE
					&& HAService.this.defaultTidalStore.getStoreService().getMySid() == 0
					&& HAService.this.defaultTidalStore.getStoreService().getBrotherSid() == 0) {
				header = HAService.this.lastStartHAConnectionTime;
			} else if (HAService.this.defaultTidalStore.getStoreService()
					.getLastOffset() == StoreHeader.STORE_HEADER_SIZE
					&& HAService.this.defaultTidalStore.getStoreService().getMySid() > 0
					&& HAService.this.defaultTidalStore.getStoreService().getBrotherSid() > 0) {
				header = 0;
			} else if (HAService.this.defaultTidalStore.getStoreService()
					.getLastOffset() > StoreHeader.STORE_HEADER_SIZE
					&& HAService.this.defaultTidalStore.getStoreService().getMySid() > 0
					&& HAService.this.defaultTidalStore.getStoreService().getBrotherSid() > 0) {
				header = 1;
			}

			int mySid = HAService.this.defaultTidalStore.getStoreService().getMySid();
			int brotherSid = HAService.this.defaultTidalStore.getStoreService().getBrotherSid();

			this.reportSid.clear();
			this.reportSid.putLong(header);
			this.reportSid.putInt(mySid);
			this.reportSid.putInt(brotherSid);
			this.reportSid.flip();

			for (int i = 0; i < 3 && this.reportSid.hasRemaining(); i++) {
				try {
					this.socketChannel.write(this.reportSid);
				} catch (IOException e) {
					log.error(this.getServiceName() + "reportLatestSid this.socketChannel.write exception", e);
					return false;
				}
			}

			log.info(this.getServiceName() + " send heartbeat!");

			return !this.reportSid.hasRemaining();
		}

		private boolean processReadEvent() {
			int readSizeZeroTimes = 0;

			while (this.byteBufferRead.hasRemaining()) {
				try {
					int readSize = this.socketChannel.read(this.byteBufferRead);
					if (readSize > 0) {
						readSizeZeroTimes = 0;
						this.byteBufferRead.flip();

						dispatchReadRequest();
						return true;
					} else if (readSize == 0) {
						if (++readSizeZeroTimes >= 3) {
							break;
						}
					} else {
						// TODO ERROR
						log.info("HAClient, processReadEvent read socket < 0");
						return false;
					}
				} catch (IOException e) {
					log.info("HAClient, processReadEvent read socket exception", e);
					return false;
				}
			}

			return true;
		}

		private void dispatchReadRequest() {
			int head = this.byteBufferRead.getInt();
			// update remotingLastOffset everytime
			HAService.this.remotingLastOffset = head;

			if (HAService.this.getDefaultTidalStore().getStoreService()
					.getLastOffset() < HAService.this.remotingLastOffset && this.byteBufferRead.getInt(4) > 0) {
				int lastComponentOffset = this.byteBufferRead.getInt();
				int length = this.byteBufferRead.getInt();
				byte[] bs = StoreComponent.get(byteBufferRead, 12, new byte[length]);

				// write header's lastComponentOffset to hard disk and memory
				HAService.this.getDefaultTidalStore().getStoreService().setLastComponentOffset(lastComponentOffset);
				// write a component to hard disk
				StoreComponent.put(HAService.this.getDefaultTidalStore().getStoreService().getHaSliceByteBuffer(),
						HAService.this.getDefaultTidalStore().getStoreService().getLastOffset(), bs);
				// update local physical file written last offset
				HAService.this.getDefaultTidalStore().getStoreService().setLastOffset(length);
				// read a component to memory
				HAService.this.getDefaultTidalStore().getStoreService().syncOffset();
				log.info("sync offset success!");
			}

			byteBufferRead.clear();
		}

		private boolean connectMaster() throws ClosedChannelException {
			if (null == socketChannel) {
				String addr = this.masterAddress.get();
				if (addr != null) {

					SocketAddress socketAddress = RemotingHelper.string2SocketAddress(addr);
					if (socketAddress != null) {
						this.socketChannel = RemotingUtil.connect(socketAddress);
						if (this.socketChannel != null) {
							this.socketChannel.register(this.selector, SelectionKey.OP_READ);
						}
					}
				}

				this.lastWriteTimestamp = System.currentTimeMillis();
			}

			return this.socketChannel != null;
		}

		private void closeMaster() {
			if (null != this.socketChannel) {
				try {

					SelectionKey sk = this.socketChannel.keyFor(this.selector);
					if (sk != null) {
						sk.cancel();
					}

					this.socketChannel.close();

					this.socketChannel = null;
				} catch (IOException e) {
					log.warn("closeMaster exception. ", e);
				}

				this.lastWriteTimestamp = 0;

				this.byteBufferRead.position(0);
				this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
			}
		}

		@Override
		public void run() {
			log.info(this.getServiceName() + " service started");

			while (!this.isStopped()) {
				try {
					if (this.connectMaster()) {

						if (this.isTimeToReportSid()) {
							boolean result = this.reportLatestSid();
							if (!result) {
								this.closeMaster();
							}

							boolean ok = this.processReadEvent();
							if (!ok) {
								this.closeMaster();
							}
							lastWriteTimestamp = HAService.this.defaultTidalStore.getSystemClock().now();
						}

						this.selector.select(1000);

						long interval = HAService.this.getDefaultTidalStore().getSystemClock().now()
								- this.lastWriteTimestamp;
						if (interval > HAService.this.getDefaultTidalStore().getStoreConfig()
								.getHaHousekeepingInterval()) {
							log.warn("HAClient, housekeeping, found this connection[" + this.masterAddress
									+ "] expired, " + interval);
							this.closeMaster();
							log.warn("HAClient, master not response some time, so close connection");
						}
					} else {
						this.waitForRunning(1000 * 5);
					}
				} catch (IOException e) {
					log.warn(this.getServiceName() + " service has exception. ", e);
					this.waitForRunning(1000 * 5);
				}
			}

			log.info(this.getServiceName() + " service end");
		}

		@Override
		public String getServiceName() {
			return HAClient.class.getSimpleName();
		}

	}
}
