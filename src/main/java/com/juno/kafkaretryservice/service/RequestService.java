package com.juno.kafkaretryservice.service;

import com.juno.kafkaretryservice.domain.Request;
import com.juno.kafkaretryservice.store.RequestStore;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.UUID;

@Service
public class RequestService {

    private final RequestStore requestStore;

    public RequestService(RequestStore requestStore) {
        this.requestStore = requestStore;
    }

    public Request createRequest(String payload) {
        Request request = new Request(payload);
        return requestStore.save(request);
    }

    public Request findById(UUID id) {
        return requestStore.findById(id);
    }

    public Collection<Request> findAll() {
        return requestStore.findAll();
    }



}