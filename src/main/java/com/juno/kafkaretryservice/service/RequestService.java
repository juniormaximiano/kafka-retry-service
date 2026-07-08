package com.juno.kafkaretryservice.service;

import com.juno.kafkaretryservice.domain.Request;
import com.juno.kafkaretryservice.dto.RequestCreateDTO;
import com.juno.kafkaretryservice.event.RequestCreatedEvent;
import com.juno.kafkaretryservice.producer.RequestProducer;
import com.juno.kafkaretryservice.store.RequestStore;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

@Service
public class RequestService {

    private final RequestStore requestStore;

    private final RequestProducer requestProducer;


    public RequestService(RequestStore requestStore, RequestProducer requestProducer) {
        this.requestStore = requestStore;
        this.requestProducer = requestProducer;
    }

    public Request createRequest(RequestCreateDTO dto) {
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