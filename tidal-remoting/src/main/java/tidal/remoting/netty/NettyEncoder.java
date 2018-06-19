package tidal.remoting.netty;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import tidal.remoting.common.LoggerName;
import tidal.remoting.common.RemotingHelper;
import tidal.remoting.common.RemotingUtil;
import tidal.remoting.protocol.RemotingCommand;

public class NettyEncoder extends MessageToByteEncoder<RemotingCommand> {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.REMOTING_LOGGER_NAME);

	@Override
	protected void encode(ChannelHandlerContext ctx, RemotingCommand remotingCommand, ByteBuf out) throws Exception {
		try {
			ByteBuffer header = remotingCommand.encodeHeader();
            out.writeBytes(header);
            byte[] body = remotingCommand.getBody();
            if (body != null) {
                out.writeBytes(body);
            }
		} catch (Exception e) {
			log.error("encode exception, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()), e);
            if (remotingCommand != null) {
                log.error(remotingCommand.toString());
            }
            RemotingUtil.closeChannel(ctx.channel());
		} 
	}

}
