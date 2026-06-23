package com.eci.arsw.eda.manager;

import com.eci.arsw.eda.consumer.AuditoriaConsumer;
import com.eci.arsw.eda.consumer.FraudeConsumer;
import com.eci.arsw.eda.consumer.NotificacionesConsumer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

@Component
public class ConsumerManager {

    private final RedisTemplate<String, String> redisTemplate;

    private FraudeConsumer        fraudeConsumer;
    private NotificacionesConsumer notifConsumer;
    private AuditoriaConsumer     auditoriaConsumer;

    public ConsumerManager(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @PostConstruct
    public void init() {
        fraudeConsumer    = new FraudeConsumer(redisTemplate);
        notifConsumer     = new NotificacionesConsumer(redisTemplate);
        auditoriaConsumer = new AuditoriaConsumer(redisTemplate);

        arrancar(fraudeConsumer,    "fraude-thread");
        arrancar(notifConsumer,     "notif-thread");
        arrancar(auditoriaConsumer, "auditoria-thread");

        System.out.println("[ConsumerManager] Consumidores iniciados");
    }

    @PreDestroy
    public void shutdown() {
        fraudeConsumer.stop();
        notifConsumer.stop();
        auditoriaConsumer.stop();
    }

    public int totalAuditados() {
        return auditoriaConsumer.total();
    }

    private void arrancar(com.eci.arsw.eda.consumer.BaseConsumer c, String nombre) {
        c.start();
        Thread t = new Thread(c, nombre);
        t.setDaemon(true);
        t.start();
    }
}
