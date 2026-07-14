package com.eci.arsw.eda.consumer;

import com.eci.arsw.eda.config.RedisStreamConfig;
import com.eci.arsw.eda.domain.TransferenciaCreada;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisTemplate;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class BaseConsumer implements Runnable {

    protected final RedisTemplate<String, String> redisTemplate;
    protected final String group;
    protected final String consumerName;

    private final AtomicBoolean running = new AtomicBoolean(false);

    protected BaseConsumer(RedisTemplate<String, String> redisTemplate,
                           String group, String consumerName) {
        this.redisTemplate = redisTemplate;
        this.group         = group;
        this.consumerName  = consumerName;
    }

    public void start() { running.set(true); }
    public void stop()  { running.set(false); }

    @Override
    public void run() {
        System.out.println("[" + consumerName + "] Escuchando...");
        while (running.get()) {
            try {
                List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream()
                        .read(Consumer.from(group, consumerName),
                                StreamReadOptions.empty().count(10).block(Duration.ofMillis(500)),
                                StreamOffset.create(RedisStreamConfig.STREAM_KEY,
                                        ReadOffset.lastConsumed()));

                if (records == null) continue;

                for (MapRecord<String, Object, Object> record : records) {
                    TransferenciaCreada event = fromMap(record.getValue());
                    onEvent(event);
                    // Confirmar que el mensaje fue procesado (XACK)
                    redisTemplate.opsForStream()
                            .acknowledge(RedisStreamConfig.STREAM_KEY, group, record.getId());
                }
            } catch (Exception e) {
                System.err.println("[" + consumerName + "] Error: " + e.getMessage());
                try { Thread.sleep(200); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    // Cada subclase define qué hace con el evento
    protected abstract void onEvent(TransferenciaCreada event);

    private TransferenciaCreada fromMap(Map<Object, Object> raw) {
        return new TransferenciaCreada(
                (String) raw.get("eventId"),
                (String) raw.get("transferId"),
                (String) raw.get("from"),
                (String) raw.get("to"),
                new BigDecimal((String) raw.get("amount")),
                (String) raw.get("currency"),
                Instant.parse((String) raw.get("createdAt"))
        );
    }
}
