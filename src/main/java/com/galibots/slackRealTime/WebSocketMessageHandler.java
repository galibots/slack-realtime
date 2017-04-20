package com.galibots.slackRealTime;

import io.netty.channel.ChannelHandlerContext;

public interface WebSocketMessageHandler {
   String handleMessage(ChannelHandlerContext ctx, String frameText);
}
