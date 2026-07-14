package com.eci.arsw.eda.domain;

import java.math.BigDecimal;
import java.time.Instant;

public record TransferenciaCreada(
        String     eventId,
        String     transferId,
        String     from,
        String     to,
        BigDecimal amount,
        String     currency,
        Instant    createdAt
) {}
