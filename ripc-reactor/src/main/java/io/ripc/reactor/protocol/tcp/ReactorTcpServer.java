package io.ripc.reactor.protocol.tcp;

import io.ripc.protocol.tcp.TcpConnection;
import io.ripc.protocol.tcp.TcpHandler;
import io.ripc.protocol.tcp.TcpServer;
import org.reactivestreams.Publisher;
import reactor.Environment;
import reactor.io.net.ReactorChannelHandler;

/**
 * Created by jbrisbin on 5/28/15.
 */
public class ReactorTcpServer<R, W> {

	static {
		Environment.initializeIfEmpty()
		           .assignErrorJournal();
	}

	private final TcpServer<R, W> transport;

	public ReactorTcpServer(TcpServer<R, W> transport) {
		this.transport = transport;
	}

	public ReactorTcpServer<R, W> start(ReactorChannelHandler<R, W, ReactorTcpConnection<R, W>> handler) {
		transport.start(new TcpHandler<R, W>() {
			@Override
			public Publisher<Void> handle(TcpConnection<R, W> connection) {
				return handler.apply(new ReactorTcpConnection<>(Environment.get(),
				                                                null,
				                                                1024,
				                                                Environment.sharedDispatcher(),
				                                                connection));
			}
		});
		return this;
	}

	public boolean shutdown() {
		return transport.shutdown();
	}

}
