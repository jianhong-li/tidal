package tidal.client.test;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import tidal.client.SendCallback;
import tidal.client.SendResult;
import tidal.client.seat.DefaultSeatClient;

@Ignore
public class SeatClientTest {

	private static DefaultSeatClient defaultSeatClient;

	@BeforeClass
	public static void setup() throws Exception {
		defaultSeatClient = new DefaultSeatClient();
		defaultSeatClient.setServerAddr("127.0.0.1:8085");
		defaultSeatClient.start();
	}

	@Test
	public void LogicDemoSync() throws Exception {
		SendResult send = defaultSeatClient.send("LogicDemo");
		System.out.println("Sync: " + send);
		assertNotNull(send.getMessage());
	}

	@Test
	public void LogicDemoAsync() throws Exception {
		defaultSeatClient.send("LogicDemo", new SendCallback() {

			@Override
			public void onSuccess(SendResult sendResult) {
				System.out.println("Async: " + sendResult);
				assertNotNull(sendResult.getMessage());
			}

			@Override
			public void onException(Throwable e) {
				e.printStackTrace();
			}
		});
		
		Thread.sleep(3000);
	}
	
	@Test
	public void LogicDemo2Sync() throws Exception {
		SendResult send = defaultSeatClient.send("LogicDemo2");
		System.out.println("Sync: " + send);
		assertNotNull(send.getMessage());
	}

	@AfterClass
	public static void close() {
		defaultSeatClient.shutdown();
	}

}
