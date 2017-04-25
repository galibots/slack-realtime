package com.galibots.slack;

import com.eclipsesource.json.JsonObject;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Callable;

class KafkaConsumerCallable implements Callable<String> {
    private SlackRtmMessageHandler slackRtmMessageHandler;

    KafkaConsumerCallable(SlackRtmMessageHandler slackRtmMessageHandler) {
        this.slackRtmMessageHandler = slackRtmMessageHandler;
        System.out.println("New KafkaConsumerCallable");
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
            while (SlackRtmMessageHandler.keepRunning.get()) {
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

                            if (message.names().contains("type") &&
                                    message.get("type").asString().equals("message") &&
                                    message.get("text").asString().equals("hello")) {

                                String slackChannel = message.get("channel").asString();

                                message = new JsonObject()
                                        .add("id", 1)
                                        .add("type", "message")
                                        .add("channel", slackChannel)
                                        .add("text", message.get("text").asString());

                                boolean channelFound = false;
                                for (Channel channel : slackRtmMessageHandler.channels) {
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
