package com.juno.kafkaretryservice.service;

import com.juno.kafkaretryservice.domain.Request;
import com.juno.kafkaretryservice.domain.RequestStatus;
import com.juno.kafkaretryservice.event.RequestCreatedEvent;
import com.juno.kafkaretryservice.store.RequestStore;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
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
            backoff = @Backoff(delay = 10000)
    )
    public void process(RequestCreatedEvent event) {

        Request request = requestStore.findById(event.requestId());

        request.setStatus(RequestStatus.PROCESSING);

        request.setAttempts(request.getAttempts() + 1);
        request.setUpdatedAt(LocalDateTime.now());

        System.out.println(
                ">> Requisição " + request.getAccessionNumber()
                        + " | Tentativa: " + request.getAttempts()
                        + " | Falha simulada: " + request.isSimulateFailure()
        );

        executarProcessamento(request);

        request.setStatus(RequestStatus.SUCCESSFUL);
        request.setUpdatedAt(LocalDateTime.now());


    }


    private void executarProcessamento(Request request) {

        if (request.isSimulateFailure()) {
            throw new RuntimeException("Erro simulado no processamento");
        }
    }


    @Recover
    public void recover(RuntimeException e, RequestCreatedEvent event) {

        Request request = requestStore.findById(event.requestId());

        request.setStatus(RequestStatus.FAILED);
        request.setUpdatedAt(LocalDateTime.now());

        System.out.println(
                ">> Requisição " + request.getAccessionNumber()
                        + " falhou definitivamente: "
                        + e.getMessage()
        );
    }

}
