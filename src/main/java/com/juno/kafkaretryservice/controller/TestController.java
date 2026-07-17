package com.juno.kafkaretryservice.controller;

import com.juno.kafkaretryservice.service.ProcessingService;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/test")
public class TestController {

    private final ProcessingService processingService;

    public TestController(ProcessingService processingService) {
        this.processingService = processingService;
    }

    @PatchMapping("/failure/{enabled}")
    public String changeFailureMode(
            @PathVariable boolean enabled
    ) {
        processingService.setForceFailure(enabled);

        return enabled
                ? "Modo de falha ativado"
                : "Modo de falha desativado";
    }
}