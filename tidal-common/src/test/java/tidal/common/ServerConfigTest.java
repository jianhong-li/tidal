package tidal.common;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ServerConfigTest {

	@Test
	public void testClientManageThreadPoolNums() {
		int expect = 2 << 4;
		assertEquals(expect, new ServerConfig().getClientManageThreadPoolNums());
	}
}
