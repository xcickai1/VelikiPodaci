package org.example;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {

    private final KafkaProducer<String, String> kafkaProducer;
    private final String topic;

    private final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());



    public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onOpen() {
        log.info("Connection opened");
    }

    @Override
    public void onClosed() {
        log.info("Connection closed");
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) {
        log.info("Received message: {}", messageEvent.getData());

        // Assuming messageEvent.getData() is a JSON string, send it to Kafka
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));

    }

    @Override
    public void onComment(String comment) {
        log.info("Received comment: {}", comment);
    }

    @Override
    public void onError(Throwable t) {
        log.error("Error occurred: {}", t.getMessage(), t);
    }
}
