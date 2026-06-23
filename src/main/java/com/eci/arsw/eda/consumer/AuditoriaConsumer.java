package com.eci.arsw.eda.consumer;

import com.eci.arsw.eda.domain.TransferenciaCreada;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AuditoriaConsumer extends BaseConsumer {

    private final List<TransferenciaCreada> log = Collections.synchronizedList(new ArrayList<>());

    public AuditoriaConsumer(RedisTemplate<String, String> redisTemplate) {
        super(redisTemplate, "auditoria-group", "auditoria-consumer-1");
    }

    @Override
    protected void onEvent(TransferenciaCreada event) {
        log.add(event);
        System.out.printf("[Auditoria] Guardado: transferencia %s por %.2f %s%n",
                event.transferId(), event.amount(), event.currency());
    }

    public int total() {
        return log.size();
    }
}
