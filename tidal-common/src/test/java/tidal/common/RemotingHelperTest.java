package tidal.common;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import tidal.remoting.common.RemotingHelper;

public class RemotingHelperTest {

	@Test
	public void testGetLocalAddress() throws Exception {
		String phyLocalAddress = RemotingHelper.getPhyLocalAddress();
		assertNotNull(phyLocalAddress);
	}
}
