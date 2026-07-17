package com.juno.kafkaretryservice.service;

import com.juno.kafkaretryservice.domain.Request;
import com.juno.kafkaretryservice.domain.RequestStatus;
import com.juno.kafkaretryservice.dto.RequestCreateDTO;
import com.juno.kafkaretryservice.event.RequestCreatedEvent;
import com.juno.kafkaretryservice.store.RequestStore;
import org.springframework.http.HttpEntity;
import org.springframework.http.MediaType;
import org.springframework.http.HttpHeaders;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDateTime;

@Service
public class ProcessingService {

    private boolean forceFailure = false;

    public void setForceFailure(boolean forceFailure) {
        this.forceFailure = forceFailure;
    }

    private final RequestStore requestStore;
    private final RestTemplate restTemplate;

    public ProcessingService(RequestStore requestStore, RestTemplate restTemplate) {
        this.requestStore = requestStore;
        this.restTemplate = restTemplate;
    }

    @Retryable(
            maxAttempts = 3,
            retryFor = RuntimeException.class,
            backoff = @Backoff(delay = 10000, multiplier = 2)
    )
    public void process(RequestCreatedEvent event) {

        Request request = requestStore.findById(event.requestId());

        request.setStatus(RequestStatus.PROCESSING);

        request.setAttempts(request.getAttempts() + 1);
        request.setUpdatedAt(LocalDateTime.now());

        System.out.println(
                ">> Requisição " + request.getAccessionNumber()
                        + " | Tentativa: " + request.getAttempts()
        );

        executarProcessamento(request);

        request.setStatus(RequestStatus.SUCCESSFUL);
        request.setUpdatedAt(LocalDateTime.now());


    }


    private void executarProcessamento(Request request) {

        RequestCreateDTO dto = new RequestCreateDTO(
                request.getAccessionNumber(),
                request.getStudyDate(),
                request.getStudyDescription(),
                request.getModality(),
                request.getPatientId(),
                request.getPatientName(),
                request.getPatientBirthDate(),
                request.getPatientSex(),
                request.getReportPdfBase64()
        );

        String url = "URL_AQUI";

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        String responseCode = forceFailure ? "500" : "200";
        headers.set("x-mock-response-code", responseCode);

        HttpEntity<RequestCreateDTO> entity =
                new HttpEntity<>(dto, headers);

        RequestCreateDTO response = restTemplate.postForObject(
                url,
                entity,
                RequestCreateDTO.class
        );

        if (response == null) {
            throw new RuntimeException(
                    "API externa retornou resposta vazia"
            );
        }

        System.out.println(
                ">> API externa respondeu para a requisição "
                        + response.accessionNumber()
        );
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
