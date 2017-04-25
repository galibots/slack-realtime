package com.galibots.slack;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.group.ChannelGroup;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;

public class SlackRtmMessageHandler implements WebSocketMessageHandler {

    // TODO: make this private
    protected final ConcurrentLinkedQueue<String> requestQueue = new ConcurrentLinkedQueue<>();

    // TODO: make this private
    protected final ChannelGroup channels;

    // TODO: make this private
    protected static final AtomicBoolean keepRunning = new AtomicBoolean(true);

    public SlackRtmMessageHandler(ChannelGroup channels) {
        this.channels = channels;

        KafkaConsumerCallable toClientCallable = new KafkaConsumerCallable(this);
        FutureTask<String> toClientPc = new FutureTask<>(toClientCallable);

        ExecutorService toClientExecutor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("to-client-%d")
                        .build()
        );
        toClientExecutor.execute(toClientPc);

        KafkaProducerCallable toKafkaCallable = new KafkaProducerCallable(this);
        FutureTask<String> toKafka = new FutureTask<>(toKafkaCallable);

        ExecutorService toKafkaExecutor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("to-kafka-%d")
                        .build()
        );
        toKafkaExecutor.execute(toKafka);
    }

    public String handleMessage(ChannelHandlerContext ctx, String frameText) {

        // TODO: here we could parse the JSON and filter by type of messages
        requestQueue.add(frameText);
        return null;
    }

}
