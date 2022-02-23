package org.apache.sysds.runtime.controlprogram.paramserv;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.apache.log4j.Logger;

public class NetworkTrafficCounter extends ChannelDuplexHandler {
    private static final Logger LOG = Logger.getLogger(NetworkTrafficCounter.class);

    private long incoming;
    private long outgoing;
    public NetworkTrafficCounter() {}

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        incoming += ((ByteBuf)msg).readableBytes();
        ctx.fireChannelRead(msg);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        LOG.debug("READ: " + incoming + "\n");
        ctx.fireChannelReadComplete();
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        outgoing += ((ByteBuf)msg).readableBytes();
        ctx.write(msg, promise);
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
        LOG.debug("WRITE: " + outgoing + "\n");
        ctx.flush();
    }
}
