package com.juno.kafkaretryservice.service;

import com.juno.kafkaretryservice.domain.Request;
import com.juno.kafkaretryservice.domain.RequestStatus;
import com.juno.kafkaretryservice.event.RequestCreatedEvent;
import com.juno.kafkaretryservice.store.RequestStore;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class ProcessingService {

    private final RequestStore requestStore;

    public ProcessingService(RequestStore requestStore) {
        this.requestStore = requestStore;
    }


    @Retryable(
            maxAttempts = 3,
            retryFor = RuntimeException.class,
            backoff = @Backoff(delay = 1000)
    )
    public void process(RequestCreatedEvent event) {

        Request request = requestStore.findById(event.requestId());

        request.setStatus(RequestStatus.PROCESSING);
        System.out.println("Processando tentativa...");
        throw new RuntimeException("Erro simulado no processamento");
//        request.setStatus(RequestStatus.SUCCESSFUL);
//        request.setUpdatedAt(LocalDateTime.now());

    }

}
