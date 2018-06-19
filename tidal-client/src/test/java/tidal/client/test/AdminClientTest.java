package tidal.client.test;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.alibaba.fastjson.JSON;

import tidal.client.AdminResult;
import tidal.client.SendStatus;
import tidal.client.admin.DefaultAdminClient;
import tidal.client.exception.TidalClientException;
import tidal.client.exception.TidalServerException;
import tidal.common.protocol.body.ComponentData;
import tidal.remoting.exception.RemotingException;

@Ignore
public class AdminClientTest {

	private static DefaultAdminClient defaultAdminClient;

	@BeforeClass
	public static void setup() throws TidalClientException {
		defaultAdminClient = new DefaultAdminClient();
		defaultAdminClient.setPassword("foobared");
		defaultAdminClient.setServerAddr("127.0.0.1:8085");
		defaultAdminClient.start();
	}

	@Test
	public void shutdownRemotingServer() throws RemotingException, InterruptedException, TidalServerException {
		AdminResult shutdownRemotingServer = defaultAdminClient.shutdownRemotingServer("192.168.1.3:8085");
		System.out.println(shutdownRemotingServer);
	}
	
	@Test
	public void createTopic1() throws RemotingException, InterruptedException, TidalServerException {
		ComponentData componentData = new ComponentData();
		componentData.setTopic("LogicDemo");
		componentData.setPloy(0);
		componentData.setInitValue(0);
		componentData.setClassName("tidal.logic.impl.LogicDemo");

		AdminResult createTopic = defaultAdminClient.createTopic(componentData);
		System.out.println(createTopic);
		assertEquals(SendStatus.SEND_OK, createTopic.getSendStatus());
	}

	@Test
	public void createTopic2() throws RemotingException, InterruptedException, TidalServerException {
		ComponentData componentData = new ComponentData();
		componentData.setTopic("LogicDemo2");
		componentData.setPloy(1);
		componentData.setInitValue(0);
		componentData.setClassName("tidal.logic.impl.LogicDemo2");

		AdminResult createTopic = defaultAdminClient.createTopic(componentData);
		System.out.println(createTopic);
		assertEquals(SendStatus.SEND_OK, createTopic.getSendStatus());
	}

	@Test
	public void queryAllTopic() throws RemotingException, InterruptedException, TidalServerException {
		AdminResult queryAllTopic = defaultAdminClient.queryAllTopic();
		List<ComponentData> parseArray = JSON.parseArray(queryAllTopic.getResult(), ComponentData.class);
		for (ComponentData componentData : parseArray) {
			System.out.println(componentData);
		}
	}

	@AfterClass
	public static void close() {
		defaultAdminClient.shutdown();
	}

}
