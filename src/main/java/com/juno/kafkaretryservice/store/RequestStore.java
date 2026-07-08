package com.juno.kafkaretryservice.store;

import com.juno.kafkaretryservice.domain.Request;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class RequestStore {

    private final Map<UUID, Request> requests = new ConcurrentHashMap<>();

    public Request save(Request request) {
        requests.put(request.getId(), request);
        return request;
    }

    public Request findById(UUID id) {
        return requests.get(id);
    }

    public List<Request> findAll() {
        return new ArrayList<>(requests.values());
    }

}
