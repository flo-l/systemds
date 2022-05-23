package org.apache.sysds.runtime.controlprogram.paramserv;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.FileRegion;
import org.apache.log4j.Logger;

import java.util.function.BiConsumer;

public class NetworkTrafficCounter extends ChannelDuplexHandler {
    private static final Logger LOG = Logger.getLogger(NetworkTrafficCounter.class);

    private final BiConsumer<Long, Long> _fn; // (read, written) -> Void, logs bytes read and written
    public NetworkTrafficCounter(BiConsumer<Long, Long> fn) {
        _fn = fn;
    }

    // adapted from AbstractTrafficShapingHandler
    private static long calculateSize(Object msg) {
        if (msg instanceof ByteBuf) {
            return ((ByteBuf)msg).readableBytes();
        } else if (msg instanceof ByteBufHolder) {
            return ((ByteBufHolder)msg).content().readableBytes();
        } else if (msg instanceof FileRegion) {
            return ((FileRegion)msg).count();
        } else {
            LOG.error("couldn't measure size of msg of type " + msg.getClass().toString() + ": " + msg);
            return 0;
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        long size = calculateSize(msg);
        if (size > 0) {
            _fn.accept(0L, size);
        }

        super.write(ctx, msg, promise);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        long size = calculateSize(msg);
        if (size > 0) {
            _fn.accept(size, 0L);
        }

        super.channelRead(ctx, msg);
    }
}
