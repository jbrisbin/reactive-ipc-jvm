package io.ripc.reactor.protocol.tcp;

import java.nio.charset.Charset;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;
import io.ripc.protocol.tcp.TcpServer;
import io.ripc.transport.netty4.tcp.Netty4TcpServer;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.rx.Promise;
import reactor.rx.Promises;
import reactor.rx.Streams;

public class CodecSample {


    public static void main(String... args) throws InterruptedException {

      echoWithLineBasedFrameDecoder();
//    echoJsonStreamDecoding();

    }

    private static void echoWithLineBasedFrameDecoder() {

        TcpServer<String, String> transport = Netty4TcpServer.<String, String>create(0,
                new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel channel) throws Exception {
                        channel.config().setOption(ChannelOption.SO_RCVBUF, 1);
                        channel.config().setOption(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(1));
                        channel.pipeline().addFirst(
                                new LineBasedFrameDecoder(256),
                                new StringDecoder(CharsetUtil.UTF_8),
                                new StringEncoder(CharsetUtil.UTF_8));
                    }
                });

// This doesn't work (consumes only 1 item):
//        ReactorTcpServer.create(transport)
//                .start(connection -> {
//                    connection.capacity(1)
//                            .flatMap(s -> connection.writeWith(Streams.just("Hello " + s + "\n")))
//                            .consume();
//                    return Streams.never();
//                });

        ReactorTcpServer.create(transport)
                .start(connection -> {
                    connection.subscribe(new Subscriber<String>() {

                        private Subscription subscription;

                        @Override
                        public void onSubscribe(Subscription subscription) {
                            this.subscription = subscription;
                            subscription.request(1);
                        }

                        @Override
                        public void onNext(String s) {
                            connection.writeWith(Streams.just("Hello " + s + "\n"))
                                    .subscribe(new Subscriber<Void>() {
                                        @Override
                                        public void onSubscribe(Subscription s) {}

                                        @Override
                                        public void onNext(Void aVoid) {}

                                        @Override
                                        public void onError(Throwable t) {}

                                        @Override
                                        public void onComplete() {
                                            subscription.request(1);
                                        }
                                    });
                        }

                        @Override
                        public void onError(Throwable t) {
                            t.printStackTrace();
                        }

                        @Override
                        public void onComplete() {
                            System.out.println("Connection input complete");
                        }
                    });
                    return Streams.never();
                });
    }

    private static void echoJsonStreamDecoding() {

        TcpServer<Person, Person> transport = Netty4TcpServer.<Person, Person>create(0,
                new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel channel) throws Exception {
                        channel.pipeline().addFirst(
                                new JsonObjectDecoder(),
                                new JsonCodec());
                    }
                });

// This doesn't work (consumes only 1 item):
//        ReactorTcpServer.create(transport)
//                .start(connection -> {
//                    Promise<Void> promise = Promises.prepare();
//                    connection.capacity(1).flatMap(inPerson -> {
//                        Person outPerson = new Person()
//                                .setFirstName(inPerson.getLastName())
//                                .setLastName(inPerson.getFirstName());
//                        return connection.writeWith(Streams.just(outPerson));
//                    }).consume();
//                    return promise;
//                });

        ReactorTcpServer.create(transport)
                .start(connection -> {
                    connection.subscribe(new Subscriber<Person>() {

                        private Subscription subscription;

                        @Override
                        public void onSubscribe(Subscription subscription) {
                            this.subscription = subscription;
                            subscription.request(1);
                        }

                        @Override
                        public void onNext(Person inPerson) {

                            Person outPerson = new Person();
                            outPerson.setFirstName(inPerson.getLastName());
                            outPerson.setLastName(inPerson.getFirstName());

                            connection.writeWith(Streams.just(outPerson))
                                    .subscribe(new Subscriber<Void>() {
                                        @Override
                                        public void onSubscribe(Subscription s) {}

                                        @Override
                                        public void onNext(Void aVoid) {}

                                        @Override
                                        public void onError(Throwable t) {}

                                        @Override
                                        public void onComplete() {
                                            subscription.request(1);
                                        }
                                    });
                        }

                        @Override
                        public void onError(Throwable t) {
                            t.printStackTrace();
                        }

                        @Override
                        public void onComplete() {
                            System.out.println("Connection input complete");
                        }
                    });
                    return Streams.never();
                });

    }


    private static class JsonCodec extends ChannelDuplexHandler {

        private final ObjectMapper mapper = new ObjectMapper();

        @Override
        public void channelRead(ChannelHandlerContext context, Object message) throws Exception {
            if (message instanceof ByteBuf) {
                Charset charset = Charset.defaultCharset();
                message = this.mapper.readValue(((ByteBuf) message).toString(charset), Person.class);
            }
            super.channelRead(context, message);
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            if (msg instanceof Person) {
                byte[] buff = mapper.writeValueAsBytes(msg);
                ByteBuf bb = ctx.alloc().buffer(buff.length);
                bb.writeBytes(buff);
                msg = bb;
            }
            super.write(ctx, msg, promise);
        }
    }

    private static class Person {

        private String firstName;

        private String lastName;

        public Person() {
        }

        public String getFirstName() {
            return firstName;
        }

        public Person setFirstName(String firstName) {
            this.firstName = firstName;
            return this;
        }

        public String getLastName() {
            return lastName;
        }

        public Person setLastName(String lastName) {
            this.lastName = lastName;
            return this;
        }
    }

}
