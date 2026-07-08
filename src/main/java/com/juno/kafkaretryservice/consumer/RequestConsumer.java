package com.juno.kafkaretryservice.consumer;

import com.juno.kafkaretryservice.event.RequestCreatedEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class RequestConsumer {

    @KafkaListener(topics = "request-created-topic", groupId = "kafka-retry-service")
    public void consume(RequestCreatedEvent event) {
        System.out.println("Mensagem recebida do Kafka: " + event);
    }
}