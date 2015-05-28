package io.ripc.reactor.protocol.tcp;

import io.ripc.protocol.tcp.TcpServer;
import reactor.Environment;
import reactor.core.dispatch.SynchronousDispatcher;
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

	ReactorTcpServer(TcpServer<R, W> transport) {
		this.transport = transport;
	}

	public ReactorTcpServer<R, W> start(ReactorChannelHandler<R, W, ReactorTcpConnection<R, W>> handler) {
		transport.startAndAwait(conn -> {
			return handler.apply(new ReactorTcpConnection<>(Environment.get(),
			                                                null,
			                                                1024,
			                                                SynchronousDispatcher.INSTANCE,
			                                                conn));
		});
		return this;
	}

	public boolean shutdown() {
		boolean b = transport.shutdown();
		transport.awaitShutdown();
		return b;
	}

	public static <R, W> ReactorTcpServer<R, W> create(TcpServer<R, W> transport) {
		return new ReactorTcpServer<>(transport);
	}

}
