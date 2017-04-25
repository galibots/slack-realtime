package com.galibots.slack;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.net.URI;

public final class SlackClient {

    public static void main(String[] args) throws Exception {

        // Authenticate in Slack
        final String slackToken = System.getenv("SLACK_TOKEN");

        if ((slackToken == null) || (slackToken == "")) {
            System.out.println("SLACK_TOKEN environment variable not defined");
            System.exit(-1);
        }

        SlackAuthenticator slackAuth = new SlackAuthenticator();
        String wssUrl = slackAuth.authenticateWithSlack(slackToken);

        // WebSocket connection
        URI uri = new URI(wssUrl);
        EventLoopGroup group = new NioEventLoopGroup(1);
        try {

            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new WebSocketSlackRtmInitializer(uri));

            // TODO: fix 443 magic number
            //Channel ch = b.connect(uri.getHost(), 443).sync().channel();
            ChannelFuture ch = b.connect(uri.getHost(), 443).sync();
            ch.channel().closeFuture().sync();

        } finally {
            group.shutdownGracefully();
        }
    }
}
