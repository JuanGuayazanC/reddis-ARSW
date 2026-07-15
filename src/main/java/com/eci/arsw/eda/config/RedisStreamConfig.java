package com.eci.arsw.eda.config;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class RedisStreamConfig {

    public static final String STREAM_KEY      = "banco.transferencias";
    public static final String GROUP_FRAUDE    = "fraude-group";
    public static final String GROUP_NOTIF     = "notif-group";
    public static final String GROUP_AUDITORIA = "auditoria-group";

    private final RedisTemplate<String, String> redisTemplate;

    public RedisStreamConfig(RedisConnectionFactory factory) {
        this.redisTemplate = new RedisTemplate<>();
        this.redisTemplate.setConnectionFactory(factory);
        this.redisTemplate.setKeySerializer(new StringRedisSerializer());
        this.redisTemplate.setValueSerializer(new StringRedisSerializer());
        this.redisTemplate.setHashKeySerializer(new StringRedisSerializer());
        this.redisTemplate.setHashValueSerializer(new StringRedisSerializer());
        this.redisTemplate.afterPropertiesSet();
    }

    @Bean
    public RedisTemplate<String, String> redisTemplate() {
        return redisTemplate;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void initStreamAndGroups() {
        for (String group : new String[]{ GROUP_FRAUDE, GROUP_NOTIF, GROUP_AUDITORIA }) {
            try {
                redisTemplate.opsForStream()
                        .createGroup(STREAM_KEY, ReadOffset.latest(), group);
                System.out.println("[Redis] Grupo creado: " + group);
            } catch (Exception e) {
                Throwable cause = e;
                boolean busyGroup = false;
                while (cause != null) {
                    if (cause.getMessage() != null && cause.getMessage().contains("BUSYGROUP")) {
                        busyGroup = true;
                        break;
                    }
                    cause = cause.getCause();
                }
                if (busyGroup) {
                    System.out.println("[Redis] Grupo ya existe: " + group);
                } else {
                    throw e;
                }
            }
        }
    }
}
