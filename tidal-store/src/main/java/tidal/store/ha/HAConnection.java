package tidal.store.ha;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tidal.common.ServiceThread;
import tidal.remoting.common.RemotingUtil;
import tidal.store.common.LoggerName;
import tidal.store.impl.StoreComponent;
import tidal.store.impl.StoreHeader;

public class HAConnection {
	private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
	private final HAService haService;
	private final SocketChannel socketChannel;
	private final String clientAddr;
	private WriteSocketService writeSocketService;
	private ReadSocketService readSocketService;

	public HAConnection(final HAService haService, final SocketChannel socketChannel) throws IOException {
		this.haService = haService;
		this.socketChannel = socketChannel;
		this.clientAddr = this.socketChannel.socket().getRemoteSocketAddress().toString();
		this.socketChannel.configureBlocking(false);
		this.socketChannel.socket().setSoLinger(false, -1);
		this.socketChannel.socket().setTcpNoDelay(true);
		this.socketChannel.socket().setReceiveBufferSize(1024 * 64);
		this.socketChannel.socket().setSendBufferSize(1024 * 64);
		this.writeSocketService = new WriteSocketService(this.socketChannel);
		this.readSocketService = new ReadSocketService(this.socketChannel);
		this.haService.getConnectionCount().incrementAndGet();
	}

	public void start() {
		this.readSocketService.start();
		this.writeSocketService.start();
	}

	public void shutdown() {
		this.writeSocketService.shutdown(true);
		this.readSocketService.shutdown(true);
		this.close();
	}

	public void close() {
		if (this.socketChannel != null) {
			try {
				this.socketChannel.close();
			} catch (IOException e) {
				HAConnection.log.error("", e);
			}
		}
	}

	public SocketChannel getSocketChannel() {
		return socketChannel;
	}

	class ReadSocketService extends ServiceThread {
		private static final int READ_MAX_BUFFER_SIZE = 16;// headerSize + remotingSelfLenght + remotingBrotherLenght =
															// READ_MAX_BUFFER_SIZE
		private final int selfOffset = 8;
		private final int brotherOffset = 12;

		private final Selector selector;
		private final SocketChannel socketChannel;
		private final ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
		private volatile long lastReadTimestamp = System.currentTimeMillis();

		public ReadSocketService(final SocketChannel socketChannel) throws IOException {
			this.selector = RemotingUtil.openSelector();
			this.socketChannel = socketChannel;
			this.socketChannel.register(this.selector, SelectionKey.OP_READ);
			this.thread.setDaemon(true);
		}

		@Override
		public void run() {
			HAConnection.log.info(this.getServiceName() + " service started");

			while (!this.isStopped()) {
				try {
					this.selector.select(1000);
					boolean ok = this.processReadEvent();
					if (!ok) {
						HAConnection.log.error("processReadEvent error");
						break;
					}

					long interval = HAConnection.this.haService.getDefaultTidalStore().getSystemClock().now()
							- this.lastReadTimestamp;
					if (interval > HAConnection.this.haService.getDefaultTidalStore().getStoreConfig()
							.getHaHousekeepingInterval()) {
						log.warn("ha housekeeping, found this connection[" + HAConnection.this.clientAddr
								+ "] expired, " + interval);
						break;
					}
				} catch (Exception e) {
					HAConnection.log.error(this.getServiceName() + " service has exception.", e);
					break;
				}
			}

			this.makeStop();

			writeSocketService.makeStop();

			haService.removeConnection(HAConnection.this);

			HAConnection.this.haService.getConnectionCount().decrementAndGet();

			SelectionKey sk = this.socketChannel.keyFor(this.selector);
			if (sk != null) {
				sk.cancel();
			}

			try {
				this.selector.close();
				this.socketChannel.close();
			} catch (IOException e) {
				HAConnection.log.error("", e);
			}

			HAConnection.log.info(this.getServiceName() + " service end");

		}

		@Override
		public String getServiceName() {
			return ReadSocketService.class.getSimpleName();
		}

		private boolean processReadEvent() {
			int readSizeZeroTimes = 0;

			if (!this.byteBufferRead.hasRemaining()) {
				this.byteBufferRead.flip();
			}

			while (this.byteBufferRead.hasRemaining()) {
				try {
					int readSize = this.socketChannel.read(this.byteBufferRead);
					if (readSize > 0) {
						readSizeZeroTimes = 0;
						this.lastReadTimestamp = HAConnection.this.haService.getDefaultTidalStore().getSystemClock()
								.now();
						if (this.byteBufferRead.position() == READ_MAX_BUFFER_SIZE) {
							long head = this.byteBufferRead.getLong(0);
							int remotingMySid = this.byteBufferRead.getInt(selfOffset);
							int remotingBrotherSid = this.byteBufferRead.getInt(brotherOffset);

							log.debug("server read head: " + head + ", mySid: " + remotingMySid
									+ ", brotherSid: " + remotingBrotherSid);

							int selfSid = HAConnection.this.haService.getDefaultTidalStore().getStoreService()
									.getMySid();
							int brocherSid = HAConnection.this.haService.getDefaultTidalStore().getStoreService()
									.getBrotherSid();
							if (remotingMySid == 0 && remotingBrotherSid == 0 && selfSid == 0
									&& brocherSid == 0) { // init
								HAConnection.this.haService.notifyTransferSome(head);
							} else if (remotingMySid > 0 && remotingBrotherSid > 0 && selfSid >= 0
									&& brocherSid >= 0) { // sync
								HAConnection.this.haService.notifyUpdateSid(head, remotingMySid,
										remotingBrotherSid);
							} else {
								// ignore
							}
						}
					} else if (readSize == 0) {
						if (++readSizeZeroTimes >= 3) {
							break;
						}
					} else {
						log.error("read socket[" + HAConnection.this.clientAddr + "] < 0");
						return false;
					}
				} catch (IOException e) {
					log.error("processReadEvent exception", e);
					return false;
				}
			}

			return true;
		}
	}

	class WriteSocketService extends ServiceThread {
		private final Selector selector;
		private final SocketChannel socketChannel;

		private final ByteBuffer transferByteBuffer = ByteBuffer.allocate(1024 * 1024 * 8);
		private long lastWriteTimestamp = System.currentTimeMillis();

		public WriteSocketService(final SocketChannel socketChannel) throws IOException {
			this.selector = RemotingUtil.openSelector();
			this.socketChannel = socketChannel;
			this.socketChannel.register(this.selector, SelectionKey.OP_WRITE);
			this.thread.setDaemon(true);
		}

		@Override
		public void run() {
			HAConnection.log.info(this.getServiceName() + " service started");

			while (!this.isStopped()) {
				try {
					this.selector.select(1000);

					if (HAConnection.this.haService.getLastStartHAConnectionTime() != -1) {
						long interval = HAConnection.this.haService.getDefaultTidalStore().getSystemClock().now()
								- this.lastWriteTimestamp;
						if (interval > HAConnection.this.haService.getDefaultTidalStore().getStoreConfig()
								.getHaSendHeartbeatInterval()) {

							boolean isTransfer = transferBuffer();

							if (isTransfer) {
								transferData();
							}
						} else {
							HAConnection.this.haService.getWaitNotifyObject().allWaitForRunning(100);
						}
					} else {
						// ignore
					}
				} catch (Exception e) {
					HAConnection.log.error(this.getServiceName() + " service has exception.", e);
					break;
				}
			}

			this.makeStop();

			readSocketService.makeStop();

			haService.removeConnection(HAConnection.this);

			SelectionKey sk = this.socketChannel.keyFor(this.selector);
			if (sk != null) {
				sk.cancel();
			}

			try {
				this.selector.close();
				this.socketChannel.close();
			} catch (IOException e) {
				HAConnection.log.error("", e);
			}

			HAConnection.log.info(this.getServiceName() + " service end");
		}

		private void transferData() throws Exception {
			int writeSizeZeroTimes = 0;
			// Write Buffer
			while (this.transferByteBuffer.hasRemaining()) {
				int writeSize = this.socketChannel.write(this.transferByteBuffer);
				if (writeSize > 0) {
					writeSizeZeroTimes = 0;
					this.lastWriteTimestamp = HAConnection.this.haService.getDefaultTidalStore().getSystemClock()
							.now();
				} else if (writeSize == 0) {
					if (++writeSizeZeroTimes >= 3) {
						break;
					}
				} else {
					throw new Exception("ha master write buffer error < 0");
				}
			}
		}

		private boolean transferBuffer() {
			this.transferByteBuffer.clear();
			if (haService.getDefaultTidalStore().getStoreService().getMySid() == 0
					|| haService.getDefaultTidalStore().getStoreService().getBrotherSid() == 0) {
				return false;
			} else if (haService.getRemotingLastOffset() == -1) {
				this.transferByteBuffer.putInt(haService.getDefaultTidalStore().getStoreService().getLastOffset());
				this.transferByteBuffer.putInt(0);
				this.transferByteBuffer.putInt(0);
				this.transferByteBuffer.flip();
				return true;
			} else if (haService.getRemotingLastOffset() == StoreHeader.STORE_HEADER_SIZE && haService
					.getDefaultTidalStore().getStoreService().getLastOffset() == StoreHeader.STORE_HEADER_SIZE) {
				return false;
			} else if(haService.getRemotingLastOffset() >= haService.getDefaultTidalStore().getStoreService()
					.getLastOffset()) {
				this.transferByteBuffer.putInt(haService.getDefaultTidalStore().getStoreService().getLastOffset());
				this.transferByteBuffer.putInt(0);
				this.transferByteBuffer.putInt(0);
				this.transferByteBuffer.flip();
				return true;
			} else if (haService.getRemotingLastOffset() < haService.getDefaultTidalStore().getStoreService()
					.getLastOffset()) {
				int length = haService.getDefaultTidalStore().getStoreService().getLastOffset()
						- haService.getRemotingLastOffset();
				byte dest[] = StoreComponent.get(
						haService.getDefaultTidalStore().getStoreService().getMappedByteBuffer(),
						haService.getRemotingLastOffset(), new byte[length]);

				this.transferByteBuffer.putInt(haService.getDefaultTidalStore().getStoreService().getLastOffset());
				this.transferByteBuffer
						.putInt(haService.getDefaultTidalStore().getStoreService().getLastComponentOffset());
				this.transferByteBuffer.putInt(length);
				this.transferByteBuffer.put(dest);
				this.transferByteBuffer.flip();
				return true;
			} else {
				return false;
			}
		}

		@Override
		public String getServiceName() {
			return WriteSocketService.class.getSimpleName();
		}

		@Override
		public void shutdown() {
			super.shutdown();
		}
	}
}
