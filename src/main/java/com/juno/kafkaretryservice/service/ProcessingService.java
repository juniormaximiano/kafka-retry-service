package com.juno.kafkaretryservice.service;

import com.juno.kafkaretryservice.domain.Request;
import com.juno.kafkaretryservice.domain.RequestStatus;
import com.juno.kafkaretryservice.event.RequestCreatedEvent;
import com.juno.kafkaretryservice.store.RequestStore;
import org.springframework.stereotype.Service;

@Service
public class ProcessingService {

    private final RequestStore requestStore;

    public ProcessingService(RequestStore requestStore) {
        this.requestStore = requestStore;
    }

    public void process(RequestCreatedEvent event) {

        Request request = requestStore.findById(event.requestId());

        request.setStatus(RequestStatus.PROCESSING);
    }

}
