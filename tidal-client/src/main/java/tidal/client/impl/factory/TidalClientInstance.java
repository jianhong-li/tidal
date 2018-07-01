package tidal.client.impl.factory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tidal.client.AbstractTidalClient;
import tidal.client.ClientConfig;
import tidal.client.exception.TidalClientException;
import tidal.client.impl.AbstractClientImpl;
import tidal.client.impl.TidalClientAPIImpl;
import tidal.client.impl.TidalClientManager;
import tidal.common.ServiceState;
import tidal.common.constant.LoggerName;
import tidal.common.protocol.heartbeat.HeartbeatData;
import tidal.remoting.common.RemotingHelper;
import tidal.remoting.netty.NettyClientConfig;

public class TidalClientInstance {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.CLIENT_LOGGER_NAME);

	private final ClientConfig clientConfig;
	private final int instanceIndex;
	private final String clientId;
	private final StringBuffer clientInstanceName = new StringBuffer();

	private final ConcurrentMap<String/* group */, AbstractClientImpl> clientTable = new ConcurrentHashMap<String, AbstractClientImpl>();
	private final NettyClientConfig nettyClientConfig;
	private final Lock lockHeartbeat = new ReentrantLock();
	private final ScheduledExecutorService scheduledExecutorService = Executors
			.newSingleThreadScheduledExecutor(new ThreadFactory() {
				@Override
				public Thread newThread(Runnable r) {
					return new Thread(r, "ClientFactoryScheduledThread");
				}
			});
	private final TidalClientAPIImpl tidalClientAPIImpl;
	private ServiceState serviceState = ServiceState.CREATE_JUST;

	public TidalClientInstance(AbstractTidalClient abstractTidalClient, int instanceIndex, String clientId) {
		this.clientConfig = abstractTidalClient.cloneClientConfig();
		this.instanceIndex = instanceIndex;

		this.nettyClientConfig = new NettyClientConfig();
		this.tidalClientAPIImpl = new TidalClientAPIImpl(this.nettyClientConfig);
		if (this.clientConfig.getServerAddr() != null) {
			this.tidalClientAPIImpl.updateServerAddressList(this.clientConfig.getServerAddr());
		}
		this.clientId = clientId;

		this.clientInstanceName.append(RemotingHelper.getPhyLocalAddress());
		this.clientInstanceName.append("-");
		this.clientInstanceName.append(instanceIndex);
	}

	public void start() throws TidalClientException {
		synchronized (this) {
			switch (this.serviceState) {
			case CREATE_JUST:
				this.serviceState = ServiceState.START_FAILED;
				if (null == this.clientConfig.getServerAddr()) {
					throw new TidalClientException("server address is null", null);
				}
				tidalClientAPIImpl.start();
				this.startScheduledTask();
				this.serviceState = ServiceState.RUNNING;
				break;
			case RUNNING:
				break;
			case SHUTDOWN_ALREADY:
				break;
			case START_FAILED:
				throw new TidalClientException(
						"The Factory object[" + this.getClientId() + "] has been created before, and failed.", null);
			default:
				break;
			}
		}
	}

	public void shutdown() {
		synchronized (this) {
			switch (this.serviceState) {
			case CREATE_JUST:
				break;
			case RUNNING:
				this.serviceState = ServiceState.SHUTDOWN_ALREADY;
				this.stopScheduledTask();
				this.tidalClientAPIImpl.shutdown();
				TidalClientManager.getInstance().removeClientFactory(this.clientId);
				log.info("the client factory [{}] shutdown OK", this.clientId);
				break;
			case SHUTDOWN_ALREADY:
				break;
			default:
				break;
			}
		}
	}

	public boolean registerClient(final String group, final AbstractClientImpl clientImpl) {
		if (null == group || null == clientImpl) {
			return false;
		}

		AbstractClientImpl prev = this.clientTable.putIfAbsent(group, clientImpl);
		if (prev != null) {
			log.warn("the client group[" + group + "] exist already.");
			return false;
		}

		return true;
	}

	public void unregisterClient(final String group) {
		this.clientTable.remove(group);
	}

	private HeartbeatData prepareHeartbeatData() {
		HeartbeatData heartbeatData = new HeartbeatData();
		heartbeatData.setClientID(this.clientId);
		return heartbeatData;
	}

	private void startScheduledTask() {
		this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				try {
					TidalClientInstance.this.sendHeartbeat();
				} catch (Exception e) {
					log.error("ScheduledTask sendHeartbeatToAllServer exception", e);
				}
			}
		}, 1000, this.clientConfig.getHeartbeatBrokerInterval(), TimeUnit.MILLISECONDS);
	}

	private void sendHeartbeat() {
		final HeartbeatData heartbeatData = this.prepareHeartbeatData();
		if (this.lockHeartbeat.tryLock()) {
			try {
				List<String> parseServerAddr = this.tidalClientAPIImpl.parseServerAddr(clientConfig.getServerAddr());
				boolean result = true;
				for (String addr : parseServerAddr) {
					boolean getResult = this.tidalClientAPIImpl.sendHearbeat(addr, heartbeatData, 3000);
					result &= getResult;
				}
				if (result) {
					log.debug("sendHeartbeatToAllServer " + clientConfig.getServerAddr() + " sucess.");
				}
			} catch (final Exception e) {
				log.error("sendHeartbeatToAllServer exception", e);
			} finally {
				this.lockHeartbeat.unlock();
			}
		} else {
			log.warn("lock heartBeat, but failed.");
		}
	}

	private void stopScheduledTask() {
		this.scheduledExecutorService.shutdown();
	}

	public TidalClientAPIImpl getTidalClientAPIImpl() {
		return tidalClientAPIImpl;
	}

	public int getInstanceIndex() {
		return instanceIndex;
	}

	public String getClientId() {
		return clientId;
	}

}
