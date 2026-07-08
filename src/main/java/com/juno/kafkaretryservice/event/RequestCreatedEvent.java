package com.juno.kafkaretryservice.event;

import java.util.UUID;

public record RequestCreatedEvent(
        UUID requestId,
        String acessionNumber,
        String patientId
) {
}
