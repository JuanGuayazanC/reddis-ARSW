package com.eci.arsw.eda.consumer;

import com.eci.arsw.eda.domain.TransferenciaCreada;
import org.springframework.data.redis.core.RedisTemplate;

import java.math.BigDecimal;

public class FraudeConsumer extends BaseConsumer {

    private static final BigDecimal UMBRAL = new BigDecimal("10000");

    public FraudeConsumer(RedisTemplate<String, String> redisTemplate) {
        super(redisTemplate, "fraude-group", "fraude-consumer-1");
    }

    @Override
    protected void onEvent(TransferenciaCreada event) {
        if (event.amount().compareTo(UMBRAL) > 0) {
            System.out.printf("[Fraude] ALERTA: transferencia %s por %.2f %s supera el umbral%n",
                    event.transferId(), event.amount(), event.currency());
        } else {
            System.out.printf("[Fraude] OK: transferencia %s por %.2f %s%n",
                    event.transferId(), event.amount(), event.currency());
        }
    }
}
