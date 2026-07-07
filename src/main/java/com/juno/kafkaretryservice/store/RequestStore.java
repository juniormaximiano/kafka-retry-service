package com.juno.kafkaretryservice.store;

import com.juno.kafkaretryservice.domain.Request;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
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

    public Collection<Request> findAll() {
        return requests.values();
    }

}
