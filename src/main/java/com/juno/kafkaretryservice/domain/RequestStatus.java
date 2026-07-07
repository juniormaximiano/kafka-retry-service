package com.juno.kafkaretryservice.domain;

public enum RequestStatus {
    PENDING,
    PROCESSING,
    SUCCESSFUL,
    FAILED
}
