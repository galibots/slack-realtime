package com.galibots.slackRealTime;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class HttpKafkaServerHandler extends SimpleChannelInboundHandler<Object> {

    private Producer<String, String> kafkaProducer;

    public HttpKafkaServerHandler(Producer<String, String> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) {

        System.out.println("TODO: llegamos aquí");

        if (msg instanceof HttpRequest) {
            HttpRequest httpRequest = (HttpRequest) msg;

            String uri = httpRequest.getUri();

            ProducerRecord<String, String> message = new ProducerRecord<>("example", uri);
            kafkaProducer.send(message);

            FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK);
            ctx.write(response).addListener(ChannelFutureListener.CLOSE);
        } else {
            System.out.println("It's not a valid request!");
        }
    }
}
