package com.juno.kafkaretryservice.controller;

import com.juno.kafkaretryservice.domain.Request;
import com.juno.kafkaretryservice.dto.RequestCreateDTO;
import com.juno.kafkaretryservice.service.RequestService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/requests")
@Tag(
        name = "Request Management",
        description = "Endpoints para criação, consulta e processamento de requisições"
)
public class RequestController {

    private final RequestService requestService;

    public RequestController(RequestService requestService) {
        this.requestService = requestService;
    }


    @PostMapping
    @Operation(
            summary = "Criar nova requisição",
            description = """
                    Cria uma nova requisição de processamento.
                    A requisição é armazenada em memória,
                    um evento é publicado no Kafka e posteriormente processado pelo Consumer.
                    """
    )
    @ApiResponse(
            responseCode = "200",
            description = "Requisição criada e enviada para processamento"
    )
    public Request createRequest(
            @RequestBody RequestCreateDTO dto
    ) {

        return requestService.createRequest(dto);
    }


    @GetMapping
    @Operation(
            summary = "Listar requisições",
            description = "Retorna todas as requisições armazenadas atualmente em memória."
    )
    @ApiResponse(
            responseCode = "200",
            description = "Lista retornada com sucesso"
    )
    public List<Request> findAll() {
        return requestService.findAll();
    }


    @GetMapping("/{id}")
    @Operation(
            summary = "Buscar requisição por ID",
            description = "Busca uma requisição específica utilizando o UUID gerado na criação."
    )
    @ApiResponse(
            responseCode = "200",
            description = "Requisição encontrada"
    )
    public Request findById(
            @PathVariable UUID id
    ) {
        return requestService.findById(id);
    }


    @PostMapping("/batch")
    @Operation(
            summary = "Criar requisições em lote",
            description = """
                    Recebe múltiplas requisições em uma única chamada.
                    Cada requisição gera um evento independente publicado no Kafka.
                    """
    )
    @ApiResponse(
            responseCode = "200",
            description = "Requisições criadas e enviadas para processamento"
    )
    public List<Request> createBatch(
            @RequestBody List<RequestCreateDTO> requests
    ) {
        return requests.stream()
                .map(requestService::createRequest)
                .toList();
    }

}