package tidal.server;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import tidal.common.ServerConfig;
import tidal.common.ThreadFactoryImpl;
import tidal.common.protocol.RequestCode;
import tidal.remoting.RemotingServer;
import tidal.remoting.netty.NettyRemotingServer;
import tidal.remoting.netty.NettyServerConfig;
import tidal.server.processor.AdminServerProcessor;
import tidal.server.processor.ClientManageProcessor;
import tidal.server.processor.DispatcherMessageProcessor;
import tidal.store.DefaultTidalStore;
import tidal.store.TidalStore;
import tidal.store.config.StoreConfig;

public class ServerController {

	private final ServerConfig serverConfig;
	private final NettyServerConfig nettyServerConfig;
	private final StoreConfig storeConfig;
	private final BlockingQueue<Runnable> dispatcherThreadPoolQueue;
	private final BlockingQueue<Runnable> clientManagerThreadPoolQueue;
	private final BlockingQueue<Runnable> adminManagerThreadPoolQueue;
	private TidalStore tidalStore;
	private RemotingServer remotingServer;
	private ExecutorService dispatcherMessageExecutor;
	private ExecutorService clientManageExecutor;
	private ExecutorService adminManageExecutor;

	public ServerController(final ServerConfig serverConfig, final NettyServerConfig nettyServerConfig,
			final StoreConfig storeConfig) {
		this.serverConfig = serverConfig;
		this.nettyServerConfig = nettyServerConfig;
		this.storeConfig = storeConfig;

		this.dispatcherThreadPoolQueue = new LinkedBlockingQueue<Runnable>(
				this.serverConfig.getDispatcherThreadPoolQueueCapacity());
		this.clientManagerThreadPoolQueue = new LinkedBlockingQueue<Runnable>(
				this.serverConfig.getClientManagerThreadPoolQueueCapacity());
		this.adminManagerThreadPoolQueue = new LinkedBlockingQueue<Runnable>(
				this.serverConfig.getAdminManagerThreadPoolQueueCapacity());
	}

	public boolean initialize() {
		boolean result = true;
		try {
			this.tidalStore = new DefaultTidalStore(storeConfig);
			if (storeConfig.isCluster()) {
				tidalStore.updateHaMasterAddress(storeConfig.getHaServerAddr());
			}
		} catch (IOException e) {
			result = false;
			e.printStackTrace();
		}

		if (result) {
			this.remotingServer = new NettyRemotingServer(nettyServerConfig);
			this.dispatcherMessageExecutor = new ThreadPoolExecutor( //
					this.serverConfig.getDispatcherMessageThreadPoolNums(), //
					this.serverConfig.getDispatcherMessageThreadPoolNums(), //
					1000 * 60, //
					TimeUnit.MILLISECONDS, //
					dispatcherThreadPoolQueue, //
					new ThreadFactoryImpl("DispatcherMessageThread_") //
			);

			this.clientManageExecutor = new ThreadPoolExecutor( //
					this.serverConfig.getClientManageThreadPoolNums(), //
					this.serverConfig.getClientManageThreadPoolNums(), //
					1000 * 60, TimeUnit.MILLISECONDS, //
					this.clientManagerThreadPoolQueue, //
					new ThreadFactoryImpl("ClientManageThread_") //
			);

			this.adminManageExecutor = new ThreadPoolExecutor( //
					this.serverConfig.getAdminManageThreadPoolNums(), //
					this.serverConfig.getAdminManageThreadPoolNums(), //
					0L, //
					TimeUnit.MILLISECONDS, //
					this.adminManagerThreadPoolQueue, //
					new ThreadFactoryImpl("AdminManageThread_") //
			);
			this.registerProcessor();
		}

		return result;
	}

	public void registerProcessor() {
		/**
		 * DispatcherMessageProcessor
		 */
		DispatcherMessageProcessor dispatcherMessageProcessor = new DispatcherMessageProcessor(this);
		this.remotingServer.registerProcessor( //
				RequestCode.DISPATCHER_MESSAGE, //
				dispatcherMessageProcessor, //
				dispatcherMessageExecutor //
		);

		/**
		 * ClientManageProcessor
		 */
		ClientManageProcessor clientManageProcessor = new ClientManageProcessor(this);
		this.remotingServer.registerProcessor( //
				RequestCode.HEART_BEAT, //
				clientManageProcessor, //
				clientManageExecutor //
		);

		/**
		 * AdminServerProcessor
		 */
		AdminServerProcessor adminManageProcessor = new AdminServerProcessor(this);
		this.remotingServer.registerProcessor( //
				RequestCode.ADMIN_SETTINGS, //
				adminManageProcessor, //
				adminManageExecutor //
		);
		this.remotingServer.registerProcessor( //
				RequestCode.SERVER_SHUTDOWN, //
				adminManageProcessor, //
				adminManageExecutor //
		);
		this.remotingServer.registerProcessor( //
				RequestCode.CREATE_TOPIC, //
				adminManageProcessor, //
				adminManageExecutor //
		);
		this.remotingServer.registerProcessor( //
				RequestCode.QUERY_ALL_TOPIC, //
				adminManageProcessor, //
				adminManageExecutor //
		);
	}

	public void start() throws Exception {
		if (this.tidalStore != null) {
			this.tidalStore.start();
		}

		if (this.remotingServer != null) {
			this.remotingServer.start();
		}
	}

	public void shutdown() {
		if (this.remotingServer != null) {
			this.remotingServer.shutdown();
		}

		if (this.tidalStore != null) {
			this.tidalStore.shutdown();
		}

		this.unregisterServerAll();
	}

	private void unregisterServerAll() {
	}

	public ServerConfig getServerConfig() {
		return serverConfig;
	}

	public NettyServerConfig getNettyServerConfig() {
		return nettyServerConfig;
	}

	public StoreConfig getStoreConfig() {
		return storeConfig;
	}

	public TidalStore getTidalStore() {
		return tidalStore;
	}

}
