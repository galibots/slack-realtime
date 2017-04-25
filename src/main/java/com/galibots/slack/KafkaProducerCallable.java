package com.galibots.slack;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.Callable;

/**
 * This class waits for command requests from the client request queue,
 * then sends them to Kafka
 */
class KafkaProducerCallable implements Callable<String> {
    private SlackRtmMessageHandler slackRtmMessageHandler;

    KafkaProducerCallable(SlackRtmMessageHandler slackRtmMessageHandler) {
        this.slackRtmMessageHandler = slackRtmMessageHandler;
        System.out.println("New KafkaProducerCallable");
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

            while (SlackRtmMessageHandler.keepRunning.get()) {
                while ((request = slackRtmMessageHandler.requestQueue.poll()) != null) {
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
