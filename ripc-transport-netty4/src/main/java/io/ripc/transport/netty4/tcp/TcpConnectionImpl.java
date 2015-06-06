package io.ripc.transport.netty4.tcp;

import java.util.concurrent.atomic.AtomicLong;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.ripc.protocol.tcp.TcpConnection;
import io.ripc.transport.netty4.tcp.ChannelToConnectionBridge.ConnectionInputSubscriberEvent;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class TcpConnectionImpl<R, W> implements TcpConnection<R, W> {

    private final Channel nettyChannel;

    public TcpConnectionImpl(Channel nettyChannel) {
        this.nettyChannel = nettyChannel;
        this.nettyChannel.config().setAutoRead(false);
    }

    @Override
    public Publisher<Void> write(final Publisher<? extends W> data) {
        return new Publisher<Void>() {
            @Override
            public void subscribe(Subscriber<? super Void> s) {
                nettyChannel.write(data).addListener(new FutureToSubscriberBridge(s));
            }
        };
    }

    @Override
    public void subscribe(Subscriber<? super R> s) {
        InputAdapter inputAdapter = new InputAdapter(s);
        nettyChannel.pipeline().fireUserEventTriggered(new ConnectionInputSubscriberEvent<>(inputAdapter));
        inputAdapter.init();
    }


    private class InputAdapter implements Subscription, Subscriber<R> {

        private final Subscriber<? super R> subscriber;

        private final AtomicLong demand = new AtomicLong(0);

        private boolean cancelled = false;


        public InputAdapter(Subscriber<? super R> subscriber) {
            this.subscriber = subscriber;
        }

        public void init() {
            subscriber.onSubscribe(this);
        }

        @Override
        public void request(long n) {
            if (n < 1) {
                cancelled = true;
                subscriber.onError(new IllegalStateException("Cannot request non-positive number."));
            }
            else if (demand.get() + n < 1) {
                demand.set(Long.MAX_VALUE);
                nettyChannel.read();
            }
            else {
                demand.addAndGet(n);
                nettyChannel.read();
            }
        }

        @Override
        public void cancel() {
            cancelled = true;
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            throw new IllegalStateException("Unexpected call to onSubscribe");
        }

        @Override
        public void onNext(R data) {
            subscriber.onNext(data);
            if (demand.decrementAndGet() > 0) {
                nettyChannel.read();
            }
        }

        @Override
        public void onError(Throwable ex) {
            cancel();
            subscriber.onError(ex);
        }

        @Override
        public void onComplete() {
            subscriber.onComplete();
        }

    }

    private static class FutureToSubscriberBridge implements ChannelFutureListener {

        private final Subscriber<? super Void> subscriber;

        public FutureToSubscriberBridge(Subscriber<? super Void> subscriber) {
            this.subscriber = subscriber;
        }

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            if (future.isSuccess()) {
                subscriber.onComplete();
            } else {
                subscriber.onError(future.cause());
            }
        }
    }

}
