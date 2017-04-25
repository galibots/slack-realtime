package com.galibots.slack;

import io.netty.channel.ChannelHandlerContext;

public interface WebSocketMessageHandler {
   String handleMessage(ChannelHandlerContext ctx, String frameText);
}
