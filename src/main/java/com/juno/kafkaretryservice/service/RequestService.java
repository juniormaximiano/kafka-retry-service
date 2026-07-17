package com.juno.kafkaretryservice.service;

import com.juno.kafkaretryservice.domain.Request;
import com.juno.kafkaretryservice.domain.RequestStatus;
import com.juno.kafkaretryservice.dto.RequestCreateDTO;
import com.juno.kafkaretryservice.event.RequestCreatedEvent;
import com.juno.kafkaretryservice.producer.RequestProducer;
import com.juno.kafkaretryservice.store.RequestStore;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Service
public class RequestService {

    private final RequestStore requestStore;
    private final RequestProducer requestProducer;
    private final FailedRequestReprocessingService reprocessingService;

    public RequestService(
            RequestStore requestStore,
            RequestProducer requestProducer,
            FailedRequestReprocessingService reprocessingService
    ) {
        this.requestStore = requestStore;
        this.requestProducer = requestProducer;
        this.reprocessingService = reprocessingService;
    }

    public Request createRequest(RequestCreateDTO dto) {

        List<Request> failedRequests = requestStore.findAll()
                .stream()
                .filter(request -> request.getStatus() == RequestStatus.FAILED)
                .toList();

        System.out.println("\n========================================");
        System.out.println(">> NOVO POST RECEBIDO");
        System.out.println(">> FAILED encontradas: " + failedRequests.size());
        System.out.println("========================================\n");



        failedRequests.forEach(request -> {

            request.setStatus(RequestStatus.PENDING);
            request.setAttempts(0);
            request.setUpdatedAt(LocalDateTime.now());

            RequestCreatedEvent failedEvent = new RequestCreatedEvent(
                    request.getId(),
                    request.getAccessionNumber(),
                    request.getPatientId()
            );

            requestProducer.send(failedEvent);

            System.out.println(
                    ">> Reprocessando requisição: "
                            + request.getAccessionNumber()
            );
        });

        Request request = new Request(
                dto.accessionNumber(),
                dto.studyDate(),
                dto.studyDescription(),
                dto.modality(),
                dto.patientId(),
                dto.patientName(),
                dto.patientBirthDate(),
                dto.patientSex(),
                dto.reportPdfBase64()
        );

        Request savedRequest = requestStore.save(request);

        RequestCreatedEvent event = new RequestCreatedEvent(
                savedRequest.getId(),
                savedRequest.getAccessionNumber(),
                savedRequest.getPatientId()
        );

        requestProducer.send(event);

        return savedRequest;
    }
    public Request findById(UUID id) {
        return requestStore.findById(id);
    }

    public List<Request> findAll() {
        return requestStore.findAll();
    }
}