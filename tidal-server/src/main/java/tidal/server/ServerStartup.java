package tidal.server;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import tidal.common.MixAll;
import tidal.common.ServerConfig;
import tidal.common.TidalVersion;
import tidal.common.constant.LoggerName;
import tidal.remoting.common.RemotingHelper;
import tidal.remoting.netty.NettyServerConfig;
import tidal.remoting.netty.NettySystemConfig;
import tidal.remoting.protocol.RemotingCommand;
import tidal.server.util.EnvironmentUtil;
import tidal.server.util.ServerUtil;
import tidal.store.config.StoreConfig;

public class ServerStartup {

	public static Properties properties = null;
	public static CommandLine commandLine = null;
	public static Logger log;

	public static void main(String[] args) {
		start(createBrokerController(args));
	}

	public static ServerController start(ServerController controller) {
		try {
			controller.start();
			String tip = "The server[" + RemotingHelper.getPhyLocalAddress() + ":"
					+ controller.getNettyServerConfig().getListenPort() + "] boot success.";

			if (null != controller.getStoreConfig().getHaServerAddr()) {
				tip += " and ha server is " + controller.getStoreConfig().getHaServerAddr();
			}

			log.info(tip);
			return controller;
		} catch (Throwable e) {
			e.printStackTrace();
			System.exit(-1);
		}

		return null;
	}

	public static ServerController createBrokerController(String[] args) {
		System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(TidalVersion.CURRENT_VERSION));

		if (null == System.getProperty(NettySystemConfig.TIDAL_REMOTING_SOCKET_SNDBUF_SIZE)) {
			NettySystemConfig.socketSndbufSize = 131072;
		}

		if (null == System.getProperty(NettySystemConfig.TIDAL_REMOTING_SOCKET_RCVBUF_SIZE)) {
			NettySystemConfig.socketRcvbufSize = 131072;
		}

		try {
			Options options = ServerUtil.buildCommandlineOptions(new Options());
			commandLine = ServerUtil.parseCmdLine("tidalServer", args, buildCommandlineOptions(options),
					new DefaultParser());
			if (null == commandLine) {
				System.exit(-1);
			}

			final ServerConfig serverConfig = new ServerConfig();
			final NettyServerConfig nettyServerConfig = new NettyServerConfig();
			final StoreConfig storeConfig = new StoreConfig();

			if (null == serverConfig.getTidalHome()) {
				System.out.printf("Please set the " + MixAll.TIDAL_HOME_ENV
						+ " variable in your environment to match the location of the Tidal installation");
				System.exit(-2);
			}

			if (commandLine.hasOption('p')) {
				EnvironmentUtil.printConfig(false, serverConfig, storeConfig);
			} else if (commandLine.hasOption('m')) {
				EnvironmentUtil.printConfig(true, serverConfig, storeConfig);
			}

			readTidalProperties(serverConfig, nettyServerConfig, storeConfig);

			String serverAddr = storeConfig.getHaServerAddr();
			if (null != serverAddr) {
				try {
					RemotingHelper.string2SocketAddress(serverAddr);
					storeConfig.setCluster(true);
				} catch (Exception e) {
					System.out.printf(
							"The Server Address[%s] illegal, please set it as follows, \"192.168.0.1:8085,192.168.0.2:8085\"%n",
							serverAddr);
					System.exit(-3);
				}
			}

			initLogback(serverConfig);

			final ServerController controller = new ServerController( //
					serverConfig, //
					nettyServerConfig, //
					storeConfig //
			);

			boolean initResult = controller.initialize();
			if (!initResult) {
				controller.shutdown();
				System.exit(-3);
			}

			Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
				private volatile boolean hasShutdown = false;
				private AtomicInteger shutdownTimes = new AtomicInteger(0);

				@Override
				public void run() {
					synchronized (this) {
						log.info("Shutdown hook was invoked, {}", this.shutdownTimes.incrementAndGet());
						if (!this.hasShutdown) {
							this.hasShutdown = true;
							long beginTime = System.currentTimeMillis();
							controller.shutdown();
							long consumingTimeTotal = System.currentTimeMillis() - beginTime;
							log.info("Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
						}
					}
				}
			}, "ShutdownHook"));
			return controller;
		} catch (Throwable t) {
			t.printStackTrace();
			System.exit(-1);
		}

		return null;
	}

	private static void readTidalProperties(final ServerConfig serverConfig, final NettyServerConfig nettyServerConfig,
			final StoreConfig storeConfig) throws Exception {
		String file = serverConfig.getTidalHome() + "/conf/tidal.properties";
		InputStream in = new BufferedInputStream(new FileInputStream(file));
		properties = new Properties();
		properties.load(in);

		MixAll.properties2Object(properties, serverConfig);
		MixAll.properties2Object(properties, nettyServerConfig);
		MixAll.properties2Object(properties, storeConfig);

		in.close();
	}

	private static void initLogback(final ServerConfig serverConfig) throws Exception {
		LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
		JoranConfigurator configurator = new JoranConfigurator();
		configurator.setContext(lc);
		lc.reset();
		configurator.doConfigure(serverConfig.getTidalHome() + "/conf/logback.xml");

		log = LoggerFactory.getLogger(LoggerName.SERVER_LOGGER_NAME);
	}

	public static Options buildCommandlineOptions(final Options options) {
		Option opt = new Option("p", "printConfigItem", false, "Print all config item");
		opt.setRequired(false);
		options.addOption(opt);

		opt = new Option("m", "printImportantConfig", false, "Print important config item");
		opt.setRequired(false);
		options.addOption(opt);

		opt = new Option("s", "shutdown", false, "shutdown Tidal Server");
		opt.setRequired(false);
		options.addOption(opt);

		return options;
	}
}
