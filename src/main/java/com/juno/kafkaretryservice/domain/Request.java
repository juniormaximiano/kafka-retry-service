package com.juno.kafkaretryservice.domain;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;

public class Request {

    private UUID id;
    private RequestStatus status;
    private int attempts;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
    private String accessionNumber;
    private LocalDateTime studyDate;
    private String studyDescription;
    private String modality;
    private String patientId;
    private String patientName;
    private LocalDate patientBirthDate;
    private String patientSex;
    private String reportPdfBase64;

    public String getAccessionNumber() {
        return accessionNumber;
    }


    public void setAccessionNumber(String accessionNumber) {
        this.accessionNumber = accessionNumber;
    }

    public LocalDateTime getStudyDate() {
        return studyDate;
    }

    public void setStudyDate(LocalDateTime studyDate) {
        this.studyDate = studyDate;
    }

    public String getStudyDescription() {
        return studyDescription;
    }

    public void setStudyDescription(String studyDescription) {
        this.studyDescription = studyDescription;
    }

    public String getModality() {
        return modality;
    }

    public void setModality(String modality) {
        this.modality = modality;
    }

    public String getPatientId() {
        return patientId;
    }

    public void setPatientId(String patientId) {
        this.patientId = patientId;
    }

    public String getPatientName() {
        return patientName;
    }

    public void setPatientName(String patientName) {
        this.patientName = patientName;
    }

    public LocalDate getPatientBirthDate() {
        return patientBirthDate;
    }

    public void setPatientBirthDate(LocalDate patientBirthDate) {
        this.patientBirthDate = patientBirthDate;
    }

    public String getPatientSex() {
        return patientSex;
    }

    public void setPatientSex(String patientSex) {
        this.patientSex = patientSex;
    }

    public String getReportPdfBase64() {
        return reportPdfBase64;
    }

    public void setReportPdfBase64(String reportPdfBase64) {
        this.reportPdfBase64 = reportPdfBase64;
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public RequestStatus getStatus() {
        return status;
    }

    public void setStatus(RequestStatus status) {
        this.status = status;
    }

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }

    public Request(
            String accessionNumber,
            LocalDateTime studyDate,
            String studyDescription,
            String modality,
            String patientId,
            String patientName,
            LocalDate patientBirthDate,
            String patientSex,
            String reportPdfBase64
    ) {
        this.id = UUID.randomUUID();
        this.accessionNumber = accessionNumber;
        this.studyDate = studyDate;
        this.studyDescription = studyDescription;
        this.modality = modality;
        this.patientId = patientId;
        this.patientName = patientName;
        this.patientBirthDate = patientBirthDate;
        this.patientSex = patientSex;
        this.reportPdfBase64 = reportPdfBase64;
        this.status = RequestStatus.PENDING;
        this.attempts = 0;
        this.createdAt = LocalDateTime.now();
        this.updatedAt = LocalDateTime.now();
    }

}
