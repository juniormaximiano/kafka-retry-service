package com.juno.kafkaretryservice.service;
import com.juno.kafkaretryservice.domain.Request;
import com.juno.kafkaretryservice.domain.RequestStatus;
import com.juno.kafkaretryservice.event.RequestCreatedEvent;
import com.juno.kafkaretryservice.producer.RequestProducer;
import com.juno.kafkaretryservice.store.RequestStore;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
public class FailedRequestReprocessingService {

    private final RequestStore requestStore;
    private final RequestProducer requestProducer;

    public FailedRequestReprocessingService(
            RequestStore requestStore,
            RequestProducer requestProducer
    ) {
        this.requestStore = requestStore;
        this.requestProducer = requestProducer;
    }

    public void reprocessFailedRequests() {

        List<Request> failedRequests = requestStore.findAll()
                .stream()
                .filter(request -> request.getStatus() == RequestStatus.FAILED)
                .toList();

        System.out.println(
                ">> Requisições FAILED encontradas: " + failedRequests.size()
        );

        failedRequests.forEach(request -> {

            request.setStatus(RequestStatus.PENDING);
            request.setAttempts(0);
            request.setUpdatedAt(LocalDateTime.now());

            RequestCreatedEvent event = new RequestCreatedEvent(
                    request.getId(),
                    request.getAccessionNumber(),
                    request.getPatientId()
            );

            requestProducer.send(event);

            System.out.println(
                    ">> Requisição "
                            + request.getAccessionNumber()
                            + " enviada novamente para o Kafka"
            );
        });
    }
}