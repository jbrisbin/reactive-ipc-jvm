package io.ripc.transport.netty4.tcp;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.ripc.protocol.tcp.TcpConnection;
import io.ripc.transport.netty4.tcp.ChannelToConnectionBridge.ConnectionInputSubscriberEvent;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

public class TcpConnectionImpl<R, W> implements TcpConnection<R, W> {

    private final Channel nettyChannel;

    public TcpConnectionImpl(Channel nettyChannel) {
        this.nettyChannel = nettyChannel;
    }

    @Override
    public Publisher<Void> write(final Publisher<W> data) {
        return new Publisher<Void>() {
            @Override
            public void subscribe(Subscriber<? super Void> s) {
                bridgeFutureToSub(nettyChannel.write(data), s);
            }
        };
    }

    @Override
    public void subscribe(Subscriber<? super R> s) {
        nettyChannel.pipeline().fireUserEventTriggered(new ConnectionInputSubscriberEvent<>(s));
    }

    private void bridgeFutureToSub(ChannelFuture future, final Subscriber<? super Void> s) {
        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    s.onComplete();
                } else {
                    s.onError(future.cause());
                }
            }
        });
    }
}
