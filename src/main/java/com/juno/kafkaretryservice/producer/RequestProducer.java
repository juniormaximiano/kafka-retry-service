package com.juno.kafkaretryservice.producer;

import com.juno.kafkaretryservice.event.RequestCreatedEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class RequestProducer {

    private final KafkaTemplate<String, RequestCreatedEvent> kafkaTemplate;

    public RequestProducer(KafkaTemplate<String, RequestCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(RequestCreatedEvent event) {
        kafkaTemplate.send("request-created-topic", event.requestId().toString(), event);
    }

}


