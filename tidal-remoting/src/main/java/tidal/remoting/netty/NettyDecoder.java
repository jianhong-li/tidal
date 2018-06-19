package tidal.remoting.netty;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import tidal.remoting.common.LoggerName;
import tidal.remoting.common.RemotingHelper;
import tidal.remoting.common.RemotingUtil;
import tidal.remoting.protocol.RemotingCommand;

public class NettyDecoder extends LengthFieldBasedFrameDecoder {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.REMOTING_LOGGER_NAME);
	private static final int FRAME_MAX_LENGTH = //
			Integer.parseInt(System.getProperty("tidal.remoting.frameMaxLength", "16777216"));

	public NettyDecoder() {
		super(FRAME_MAX_LENGTH, 0, 4, 0, 4);
	}

	@Override
	public Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
		ByteBuf frame = null;
		try {
			frame = (ByteBuf) super.decode(ctx, in);
			if (null == frame) {
				return null;
			}

			ByteBuffer byteBuffer = frame.nioBuffer();

			return RemotingCommand.decode(byteBuffer);
		} catch (Exception e) {
			log.error("decode exception, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()), e);
			RemotingUtil.closeChannel(ctx.channel());
		} finally {
			if (null != frame) {
				frame.release();
			}
		}

		return null;
	}

}
