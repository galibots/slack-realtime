package com.galibots.slackRealTime;

import com.eclipsesource.json.JsonObject;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.group.ChannelGroup;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;

public class SlackRtmMessageHandler implements WebSocketMessageHandler {

    // stateless JSON serializer/deserializer
    private Gson gson = new Gson();

    private static final AtomicBoolean keepRunning = new AtomicBoolean(true);

    private final ConcurrentLinkedQueue<String> requestQueue = new ConcurrentLinkedQueue<>();

    private final ChannelGroup channels;

    public SlackRtmMessageHandler(ChannelGroup channels) {
        this.channels = channels;

        ReadIncomingCallable toClientCallable = new ReadIncomingCallable();
        FutureTask<String> toClientPc = new FutureTask<>(toClientCallable);

        ExecutorService toClientExecutor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("to-client-%d")
                        .build()
        );
        toClientExecutor.execute(toClientPc);

        SendCommandsToKafkaCallable toKafkaCallable = new SendCommandsToKafkaCallable();
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

    /**
     * This class waits for command requests from the client request queue,
     * then sends them to Kafka
     */
    private class SendCommandsToKafkaCallable implements Callable<String> {
        SendCommandsToKafkaCallable() {
            System.out.println("New SendCommandsToKafkaCallable");
        }

        @Override
        public String call() throws Exception {
            // Apache Kafka connection
            Properties kafkaProperties = new Properties();
            kafkaProperties.put("bootstrap.servers", "localhost:9092");
            kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            // set up the producer
            KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProperties);

            try {
                String request;

                while (keepRunning.get()) {
                    while ((request = requestQueue.poll()) != null) {
                        String topic = "example";

                        producer.send(new ProducerRecord<>(topic, request),
                                (metadata, e) -> {
                                    if (e != null) {
                                        System.out.println(e.getMessage());
                                    }

                                    System.out.println("The offset of the record we just sent is: " + metadata.offset());
                                });
                    }
                }

                producer.flush();
                Thread.sleep(20L);
            } catch (Throwable throwable) {
                System.out.println(throwable.getMessage());
            } finally {
                producer.close();
            }

            return "done";
        }
    }

    private class ReadIncomingCallable implements Callable<String> {
        ReadIncomingCallable() {
            System.out.println("New ReadIncomingCallable");
        }

        @Override
        public String call() throws Exception {

            // Apache Kafka connection
            Properties kafkaProperties = new Properties();
            kafkaProperties.put("bootstrap.servers", "localhost:9092");
            kafkaProperties.put("group.id", "slack-real-time");
            kafkaProperties.put("enable.auto.commit", false);
            kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            kafkaProperties.put("session.timeout.ms", 10000);
            kafkaProperties.put("fetch.min.bytes", 50000);
            kafkaProperties.put("receive.buffer.bytes", 262144);
            kafkaProperties.put("max.partition.fetch.bytes", 2097152);

            // set up the consumer
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProperties);

            consumer.subscribe(Arrays.asList("example"));
            int timeouts = 0;

            try {
                while (keepRunning.get()) {
                    // read records with a short timeout. If we time out, we don't really care.
                    ConsumerRecords<String, String> records = consumer.poll(200);
                    if (records.count() == 0) {
                        timeouts++;
                    } else {
                        System.out.println(String.format("Got %d records after %d timeouts\n", records.count(), timeouts));
                        timeouts = 0;
                    }

                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println(String.format("Record for topic %s on partition %d offset %d is: %s\n", record.topic(), record.partition(), record.offset(), record.value()));
                        switch (record.topic()) {
                            case "example":
                                try {
                                    consumer.commitSync();
                                } catch (CommitFailedException e) {
                                    System.out.println("commit failed " + e.getMessage());
                                }

                                JsonObject message = JsonObject.readFrom(record.value());

                                if (message.names().contains("type") && message.get("type").asString().equals("message")) {

                                    String slackChannel = message.get("channel").asString();

                                    message = new JsonObject()
                                            .add("id", 1)
                                            .add("type", "message")
                                            .add("channel", slackChannel)
                                            .add("text", message.get("text").asString());

                                    boolean channelFound = false;
                                    for (Channel channel : channels) {
                                        channelFound = true;
                                        channel.writeAndFlush(new TextWebSocketFrame(message.toString()));
                                    }

                                    if (!channelFound) {
                                        System.out.println("Can't get channel because id wasn't set!");
                                    }
                                }
                                break;

                            default:
                                try {
                                    consumer.commitSync();
                                } catch (CommitFailedException e) {
                                    System.out.println("commit failed " + e.getMessage());
                                }

                                throw new IllegalStateException("Shouldn't be possible to get message on topic " + record.topic());
                        }
                    }
                }
            } catch (Throwable t) {
                t.printStackTrace();
            } finally {
                consumer.close();
            }

            return "done";
        }
    }
}
