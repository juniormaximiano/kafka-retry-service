package com.juno.kafkaretryservice.consumer;

import com.juno.kafkaretryservice.event.RequestCreatedEvent;
import com.juno.kafkaretryservice.service.ProcessingService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class RequestConsumer {

    private final ProcessingService processingService;

    public RequestConsumer(ProcessingService processingService) {
        this.processingService = processingService;
    }

    @KafkaListener(
            topics = "request-created-topic",
            groupId = "kafka-retry-service"
    )
    public void consume(RequestCreatedEvent event) {

        processingService.process(event);

    }
}