package com.eci.arsw.eda.consumer;

import com.eci.arsw.eda.domain.TransferenciaCreada;
import org.springframework.data.redis.core.RedisTemplate;

public class NotificacionesConsumer extends BaseConsumer {

    public NotificacionesConsumer(RedisTemplate<String, String> redisTemplate) {
        super(redisTemplate, "notif-group", "notif-consumer-1");
    }

    @Override
    protected void onEvent(TransferenciaCreada event) {
        System.out.printf("[Notificaciones] Enviando notificacion a '%s': recibiste %.2f %s de '%s'%n",
                event.to(), event.amount(), event.currency(), event.from());
    }
}
