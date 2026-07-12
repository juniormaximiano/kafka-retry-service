package com.juno.kafkaretryservice.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.LocalDate;
import java.time.LocalDateTime;

public record RequestCreateDTO(
        @NotBlank
        String accessionNumber,

        @NotNull
        LocalDateTime studyDate,

        @NotBlank
        String studyDescription,

        @NotBlank
        String modality,

        @NotBlank
        String patientId,

        @NotBlank
        String patientName,

        @NotNull
        LocalDate patientBirthDate,

        @NotBlank
        String patientSex,

        String reportPdfBase64

) {
}
