package com.galibots.slack;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.ContinuationWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.GlobalEventExecutor;

public class WebSocketSlackRtmHandler extends SimpleChannelInboundHandler<Object> {

    private final WebSocketClientHandshaker handshaker;
    private ChannelPromise handshakeFuture;
    private StringBuilder frameBuffer = null;

    public WebSocketSlackRtmHandler(WebSocketClientHandshaker handshaker) {
        this.handshaker = handshaker;
    }

    private static final ChannelGroup channels = new DefaultChannelGroup("galibotChannelGroup", GlobalEventExecutor.INSTANCE);

    private final WebSocketMessageHandler wsMessageHandler = new SlackRtmMessageHandler(channels);

    public ChannelFuture handshakeFuture() {
        return handshakeFuture;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        handshakeFuture = ctx.newPromise();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        handshaker.handshake(ctx.channel());
        channels.add(ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        channels.remove(ctx.channel());
        System.out.println("WebSocket Client disconnected!");
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {

        Channel ch = ctx.channel();
        if (!handshaker.isHandshakeComplete()) {
            handshaker.finishHandshake(ch, (FullHttpResponse) msg);
            System.out.println("WebSocket Client connected!");
            handshakeFuture.setSuccess();
            return;
        }

        if (msg instanceof WebSocketFrame) {
            System.out.println("WebSocket Packet received!");
            this.handleWebSocketFrame(ctx, (WebSocketFrame) msg);
        }

        if (msg instanceof FullHttpResponse) {
            FullHttpResponse response = (FullHttpResponse) msg;
            throw new IllegalStateException(
                    "Unexpected FullHttpResponse (getStatus=" + response.status() +
                            ", content=" + response.content().toString(CharsetUtil.UTF_8) + ')');
        }
    }

    protected void handleWebSocketFrame(ChannelHandlerContext ctx, WebSocketFrame frame) {

        Channel ch = ctx.channel();

        if (frame instanceof PongWebSocketFrame) {
            System.out.println("WebSocket Client received pong");
            return;
        }

        if (frame instanceof PingWebSocketFrame) {
            ctx.channel().writeAndFlush(new PongWebSocketFrame(frame.content().retain()));
            return;
        }

        if (frame instanceof TextWebSocketFrame) {
            frameBuffer = new StringBuilder();
            frameBuffer.append(((TextWebSocketFrame) frame).text());
        } else if (frame instanceof ContinuationWebSocketFrame) {
            if (frameBuffer != null) {
                frameBuffer.append(((ContinuationWebSocketFrame) frame).text());
            } else {
                System.out.println("Continuation frame received without initial frame.");
            }
        } else {
            throw new UnsupportedOperationException(String.format("%s frame types not supported", frame.getClass().getName()));
        }

        // Check if Text or Continuation Frame is final fragment and handle if needed.
        if (frame.isFinalFragment()) {
            handleMessageCompleted(ctx, frameBuffer.toString());
            frameBuffer = null;
        }

        // TODO: check if this is needed
/*        if (frame instanceof CloseWebSocketFrame) {
            if (frameBuffer != null) {
                handleMessageCompleted(ctx, frameBuffer.toString());
            }
            handshaker.close(ctx.channel(), (CloseWebSocketFrame) frame.retain());
            return;
        }*/

        if (frame instanceof CloseWebSocketFrame) {
            System.out.println("WebSocket Client received closing");
            ch.close();
        }
    }

    protected void handleMessageCompleted(ChannelHandlerContext ctx, String frameText) {
        wsMessageHandler.handleMessage(ctx, frameText);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        if (!handshakeFuture.isDone()) {
            handshakeFuture.setFailure(cause);
        }
        ctx.close();
    }
}
