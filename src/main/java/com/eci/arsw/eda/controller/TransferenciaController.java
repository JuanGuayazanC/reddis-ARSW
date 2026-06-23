package com.eci.arsw.eda.controller;

import com.eci.arsw.eda.domain.TransferenciaCreada;
import com.eci.arsw.eda.manager.ConsumerManager;
import com.eci.arsw.eda.producer.TransferenciaProducer;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/transferencias")
public class TransferenciaController {

    private final TransferenciaProducer producer;
    private final ConsumerManager       manager;

    public TransferenciaController(TransferenciaProducer producer, ConsumerManager manager) {
        this.producer = producer;
        this.manager  = manager;
    }

    // Publica un evento en el stream. Body opcional (usa valores demo si no se envía).
    @PostMapping
    public ResponseEntity<Map<String, Object>> publicar(
            @RequestBody(required = false) Map<String, String> body) {

        String from     = get(body, "from",     "ACC-001");
        String to       = get(body, "to",       "ACC-002");
        String amount   = get(body, "amount",   "500.00");
        String currency = get(body, "currency", "COP");

        TransferenciaCreada event = new TransferenciaCreada(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                from, to,
                new BigDecimal(amount),
                currency,
                Instant.now()
        );

        RecordId recordId = producer.publish(event);

        return ResponseEntity.ok(Map.of(
                "eventId",    event.eventId(),
                "transferId", event.transferId(),
                "recordId",   recordId.toString(),
                "amount",     event.amount(),
                "currency",   event.currency()
        ));
    }

    // Cuántas transferencias guardó el consumidor de auditoría
    @GetMapping("/auditados")
    public ResponseEntity<Map<String, Object>> auditados() {
        return ResponseEntity.ok(Map.of("totalAuditados", manager.totalAuditados()));
    }

    private String get(Map<String, String> body, String key, String def) {
        return body == null ? def : body.getOrDefault(key, def);
    }
}
