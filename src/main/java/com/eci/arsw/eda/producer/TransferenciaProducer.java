package com.eci.arsw.eda.producer;

import com.eci.arsw.eda.config.RedisStreamConfig;
import com.eci.arsw.eda.domain.TransferenciaCreada;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class TransferenciaProducer {

    private final RedisTemplate<String, String> redisTemplate;

    public TransferenciaProducer(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public RecordId publish(TransferenciaCreada event) {
        Map<String, String> fields = new HashMap<>();
        fields.put("eventId",    event.eventId());
        fields.put("transferId", event.transferId());
        fields.put("from",       event.from());
        fields.put("to",         event.to());
        fields.put("amount",     event.amount().toPlainString());
        fields.put("currency",   event.currency());
        fields.put("createdAt",  event.createdAt().toString());

        RecordId recordId = redisTemplate.opsForStream()
                .add(StreamRecords.newRecord()
                        .in(RedisStreamConfig.STREAM_KEY)
                        .ofMap(fields));

        System.out.println("[Productor] Evento publicado en Redis -> " + recordId);
        return recordId;
    }
}
