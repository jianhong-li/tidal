package tidal.remoting.protocol.test;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import tidal.remoting.CommandCustomHeader;
import tidal.remoting.protocol.RemotingCommand;
import tidal.remoting.protocol.RemotingSysResponseCode;
import tidal.remoting.test.SampleCommandCustomHeader;

public class RemotingCommandTest {

	@Test
	public void testCreateRequestCommand() {
		System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, "4753");

		int code = 256;
		CommandCustomHeader header = new SampleCommandCustomHeader();
		RemotingCommand cmd = RemotingCommand.createRequestCommand(code, header);
		assertEquals(code, cmd.getCode());
		assertEquals(4753, cmd.getVersion());
		assertEquals(0, cmd.getFlag());
	}

	@Test
	public void testCreateResponseCommand() {
		System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, "4753");

		int code = RemotingSysResponseCode.SUCCESS;
		String remark = "Sample remark";
		RemotingCommand cmd = RemotingCommand.createResponseCommand(code, remark, SampleCommandCustomHeader.class);
		System.out.println(cmd.getCode());
		assertEquals(code, cmd.getCode());
		assertEquals(4753, cmd.getVersion());
		assertEquals(remark, cmd.getRemark());
		assertEquals(1, cmd.getFlag());
	}

}
