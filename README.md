# EDA with Redis Streams — Banking Demo

Example of an event-driven architecture (EDA) built with Spring Boot and Redis
Streams. One party **produces** a bank transfer event, Redis carries it, and
three independent parties **consume** it.

```
Producer  ──XADD──►  Redis Stream  ──XREADGROUP──►  FraudeConsumer
                                                  ──XREADGROUP──►  NotificacionesConsumer
                                                  ──XREADGROUP──►  AuditoriaConsumer
```

---

## How it works

### Producer
`TransferenciaProducer` receives a `TransferenciaCreada` event and publishes it
to the `banco.transferencias` stream with `XADD`. Redis assigns it a unique ID.

### Redis Stream
Acts as the message channel. It has three **consumer groups** (one per
consumer). Each group receives every message independently.

### Consumers
Each one runs on its own thread, reads with `XREADGROUP`, and confirms with
`XACK`.

| Consumer                 | What it does                                     |
|---------------------------|--------------------------------------------------|
| `FraudeConsumer`          | Raises an alert if the amount exceeds 10,000     |
| `NotificacionesConsumer`  | Simulates a notification to the recipient        |
| `AuditoriaConsumer`       | Stores the event in an in-memory list            |

---

## Project structure

```
com.eci.arsw.eda
├── domain/      TransferenciaCreada  (the event)
├── config/      RedisStreamConfig    (creates the stream and groups on startup)
├── producer/    TransferenciaProducer
├── consumer/    BaseConsumer, FraudeConsumer, NotificacionesConsumer, AuditoriaConsumer
├── manager/     ConsumerManager      (launches the threads)
└── controller/  TransferenciaController
```

---

## Start Redis with Docker

```bash
docker run --name redis-eda -p 6379:6379 -d redis:7
```

---

## Run the application

```bash
mvn spring-boot:run
```

---

## Endpoints

| Method | URL                              | Description                        |
|--------|-----------------------------------|-------------------------------------|
| POST   | `/api/transferencias`             | Publishes an event (optional body) |
| GET    | `/api/transferencias/auditados`   | Total stored by the audit consumer |

---

## Try it with curl

**Publish a transfer:**
```bash
curl -s -X POST http://localhost:8080/api/transferencias \
  -H "Content-Type: application/json" \
  -d '{"from":"ACC-001","to":"ACC-002","amount":"500.00","currency":"COP"}'
```

**Without a body (uses demo values):**
```bash
curl -s -X POST http://localhost:8080/api/transferencias
```

**Publish a suspicious transfer (triggers the fraud alert):**
```bash
curl -s -X POST http://localhost:8080/api/transferencias \
  -H "Content-Type: application/json" \
  -d '{"from":"ACC-001","to":"ACC-099","amount":"50000.00","currency":"COP"}'
```

**Check how many transfers `AuditoriaConsumer` has audited:**
```bash
curl http://localhost:8080/api/transferencias/auditados
```

---

## Expected console output

```
[Productor]       Evento publicado en Redis -> 1750710234567-0
[Fraude]          OK: transferencia abc-123 por 500.00 COP
[Notificaciones]  Enviando notificacion a 'ACC-002': recibiste 500.00 COP de 'ACC-001'
[Auditoria]       Guardado: transferencia abc-123 por 500.00 COP
```
*(Log tags and messages are printed in Spanish by the application itself; see
[main.java source](src/main/java/com/eci/arsw/eda) for the exact strings.)*

---

## Evidence from this lab (Redis Streams)

Actual run from `2026-07-15`, with Redis 7 running in Docker (`redis-eda`) and
the app started with `mvn spring-boot:run`.

*(The raw command output blocks below are the application's real console
output and Redis' real reply — they were captured verbatim and left untranslated
so they remain checkable evidence, not a paraphrase.)*

### 1. Startup: creating/verifying the 3 consumer groups

```
[ConsumerManager] Consumidores iniciados
[notif-consumer-1] Escuchando...
[auditoria-consumer-1] Escuchando...
[fraude-consumer-1] Escuchando...
[Redis] Grupo ya existe: fraude-group
[Redis] Grupo ya existe: notif-group
[Redis] Grupo ya existe: auditoria-group
```

### 2. Full flow: publish → persist → consume → acknowledge

**Request 1 — regular transfer:**
```bash
curl -s -X POST http://localhost:8080/api/transferencias \
  -H "Content-Type: application/json" \
  -d '{"from":"ACC-001","to":"ACC-002","amount":"500.00","currency":"COP"}'
```
```json
{"recordId":"1784093069684-0","currency":"COP","amount":500.00,"eventId":"60964b9b-db0d-4cb9-82b6-dd0ae3f36507","transferId":"c8f49d25-0fb3-4c74-8ca6-281001d9f908"}
```

**Request 2 — suspicious transfer (triggers the fraud alert):**
```bash
curl -s -X POST http://localhost:8080/api/transferencias \
  -H "Content-Type: application/json" \
  -d '{"from":"ACC-001","to":"ACC-099","amount":"50000.00","currency":"COP"}'
```
```json
{"recordId":"1784093069844-0","currency":"COP","amount":50000.00,"eventId":"16df3efa-5768-43c2-bfd6-1b9dbe17b5dc","transferId":"b513711e-f152-4ac8-bca2-bdf32a0d5fa1"}
```

**Real app console output (all three consumers react to each event, each with
its own group):**
```
[Productor] Evento publicado en Redis -> 1784093069684-0
[Notificaciones] Enviando notificacion a 'ACC-002': recibiste 500,00 COP de 'ACC-001'
[Auditoria] Guardado: transferencia c8f49d25-0fb3-4c74-8ca6-281001d9f908 por 500,00 COP
[Fraude] OK: transferencia c8f49d25-0fb3-4c74-8ca6-281001d9f908 por 500,00 COP
[Productor] Evento publicado en Redis -> 1784093069844-0
[Auditoria] Guardado: transferencia b513711e-f152-4ac8-bca2-bdf32a0d5fa1 por 50000,00 COP
[Fraude] ALERTA: transferencia b513711e-f152-4ac8-bca2-bdf32a0d5fa1 por 50000,00 COP supera el umbral
[Notificaciones] Enviando notificacion a 'ACC-099': recibiste 50000,00 COP de 'ACC-001'
```

**Checking the audit consumer:**
```bash
curl -s http://localhost:8080/api/transferencias/auditados
# {"totalAuditados":2}
```

### 3. Suggested activity: simulate a consumer crashing before `XACK`

To avoid interfering with the app's real groups (`fraude-group`,
`notif-group`, `auditoria-group`), the simulation used a disposable consumer
group (`demo-crash-group`) on the same `banco.transferencias` stream, driven
directly with `redis-cli` against the container.

**Step 0 — create the demo group and publish a fresh event:**
```bash
docker exec redis-eda redis-cli XGROUP CREATE banco.transferencias demo-crash-group '$'
curl -s -X POST http://localhost:8080/api/transferencias \
  -H "Content-Type: application/json" \
  -d '{"from":"ACC-777","to":"ACC-888","amount":"12345.00","currency":"COP"}'
```
```
OK
{"recordId":"1784093093854-0", ...}
```

**Step 1 — `crash-consumer-1` reads the event but "dies" before calling `XACK`:**
```bash
docker exec redis-eda redis-cli XREADGROUP GROUP demo-crash-group crash-consumer-1 \
  COUNT 1 STREAMS banco.transferencias '>'
```
```
banco.transferencias
1784093093854-0
eventId d03fc416-6a85-4a1b-8f5d-6efa8de179a9
amount  12345.00
from    ACC-777
to      ACC-888
...
```

**Step 2 — confirm the event is stuck pending (unacknowledged):**
```bash
docker exec redis-eda redis-cli XPENDING banco.transferencias demo-crash-group
```
```
1                          # 1 pending message
1784093093854-0            # lowest ID
1784093093854-0            # highest ID
crash-consumer-1            1   # assigned to crash-consumer-1, never acknowledged
```

**Step 3 — `recovery-consumer-1` claims the orphaned message with `XCLAIM` and
acknowledges it:**
```bash
docker exec redis-eda redis-cli XCLAIM banco.transferencias demo-crash-group recovery-consumer-1 0 1784093093854-0
docker exec redis-eda redis-cli XACK banco.transferencias demo-crash-group 1784093093854-0
```

**Step 4 — no pending messages remain:**
```bash
docker exec redis-eda redis-cli XPENDING banco.transferencias demo-crash-group
# (empty)
```

**Conclusion of the simulation:** this confirms what the theory says (slides
8-9 of the lecture) — Redis Streams does not lose the event if a consumer
crashes before `XACK`; it stays visible in `XPENDING`, and any other consumer
in the group can claim it with `XCLAIM` and retry processing without
duplicating the original publish.
