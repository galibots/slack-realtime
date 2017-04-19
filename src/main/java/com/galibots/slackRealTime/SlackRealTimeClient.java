package com.galibots.slackRealTime;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketClientCompressionHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Properties;

public final class SlackRealTimeClient {

    public static void main(String[] args) throws Exception {

        // Authenticate in Slack
        final String slackToken = System.getenv("SLACK_TOKEN");

        if ((slackToken == null) || (slackToken == "")) {
            System.out.println("SLACK_TOKEN environment variable not defined");
            System.exit(-1);
        }

        SlackAuthenticator slackAuth = new SlackAuthenticator();
        String wssUrl = slackAuth.authenticateWithSlack(slackToken);

        // Apache Kafka connection
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("metadata.broker.list", "localhost:9092");
        kafkaProperties.setProperty("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig kafkaConfig = new ProducerConfig(kafkaProperties);
        Producer<String, String> kafkaProducer = new Producer<>(kafkaConfig);


        // WebSocket connection
        URI uri = new URI(wssUrl);
        EventLoopGroup group = new NioEventLoopGroup();
        try {

            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new WebSocketSlackRtmInitializer(uri));
                    //).childHandler(new HttpKafkaServerHandler(kafkaProducer));

            //pipeline.addLast();
            //Channel ch = b.connect(uri.getHost(), uri.getPort()).sync().channel();
            // TODO: fix 443 magic number
            Channel ch = b.connect(uri.getHost(), 443).sync().channel();

            BufferedReader console = new BufferedReader(new InputStreamReader(System.in));
            while (true) {
                String msg = console.readLine();

                if (msg == null) {
                    break;
                } else if ("bye".equals(msg.toLowerCase())) {
                    ch.writeAndFlush(new CloseWebSocketFrame());
                    ch.closeFuture().sync();
                    break;
                } else if ("ping".equals(msg.toLowerCase())) {
                    WebSocketFrame frame = new PingWebSocketFrame(Unpooled.wrappedBuffer(new byte[]{8, 1, 8, 1}));
                    ch.writeAndFlush(frame);
                } else {
                    WebSocketFrame frame = new TextWebSocketFrame(msg);
                    ch.writeAndFlush(frame);
                }
            }
        } finally {
            group.shutdownGracefully();
        }
    }
}
