package com.juno.kafkaretryservice.controller;

import com.juno.kafkaretryservice.domain.Request;
import com.juno.kafkaretryservice.dto.RequestCreateDTO;
import com.juno.kafkaretryservice.service.RequestService;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/requests")
public class RequestController {

    private RequestService requestService;

    public RequestController(RequestService requestService) {
        this.requestService = requestService;
    }

    @PostMapping
    public Request createRequest(RequestCreateDTO dto) {
        return requestService.createRequest(dto);
    }

    @GetMapping
    public List<Request> findAll() {
        return requestService.findAll();
    }

    @GetMapping("/{id}")
    public Request findById(@PathVariable UUID id) {
        return requestService.findById(id);
    }

    @PostMapping("/batch")
    public List<Request> createBatch(
            @RequestBody List<RequestCreateDTO> requests
    ) {
        return requests.stream()
                .map(requestService::createRequest)
                .toList();
    }


}
