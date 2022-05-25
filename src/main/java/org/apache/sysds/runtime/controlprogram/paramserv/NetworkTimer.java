package org.apache.sysds.runtime.controlprogram.paramserv;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.sysds.runtime.controlprogram.parfor.stat.Timing;

import java.util.function.Consumer;

public class NetworkTimer extends ChannelInboundHandlerAdapter {
    private final Timing _timer = new Timing();
    private final Consumer<Double> _log_time;

    public NetworkTimer(Consumer<Double> log_time) {
        _log_time = log_time;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        _timer.start();
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        _log_time.accept(_timer.stop());
        super.channelInactive(ctx);
    }
}
