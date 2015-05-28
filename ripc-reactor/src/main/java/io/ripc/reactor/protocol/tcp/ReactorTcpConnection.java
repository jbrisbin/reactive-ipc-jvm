package io.ripc.reactor.protocol.tcp;

import io.ripc.protocol.tcp.TcpConnection;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.Environment;
import reactor.core.Dispatcher;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.net.ChannelStream;

import java.net.InetSocketAddress;

/**
 * Created by jbrisbin on 5/28/15.
 */
public class ReactorTcpConnection<R, W> extends ChannelStream<R, W> {

	private final TcpConnection<R, W> transport;

	public ReactorTcpConnection(Environment env,
	                            Codec<Buffer, R, W> codec,
	                            long prefetch,
	                            Dispatcher eventsDispatcher,
	                            TcpConnection<R, W> transport) {
		super(env, codec, prefetch, eventsDispatcher);
		this.transport = transport;
	}

	@Override
	public Object delegate() {
		return transport;
	}

	@Override
	protected void doSubscribeWriter(Publisher<? extends W> writer, Subscriber<? super Void> postWriter) {
		Publisher<Void> p = transport.write(writer);
		if (null != postWriter) {
			p.subscribe(postWriter);
		}
	}

	@Override
	protected void doDecoded(R r) {

	}

	@Override
	public InetSocketAddress remoteAddress() {
		return null;
	}

	@Override
	public ConsumerSpec on() {
		return null;
	}

	@Override
	public void subscribe(Subscriber<? super R> sub) {
		transport.subscribe(sub);
	}

}
