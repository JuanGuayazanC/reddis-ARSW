# EDA con Redis Streams — Demo Bancaria

Ejemplo de arquitectura orientada por eventos (EDA) con Spring Boot y Redis Streams.
Un ente **produce** un evento de transferencia bancaria, Redis lo transporta,
y tres entes independientes lo **consumen**.

```
Productor  ──XADD──►  Redis Stream  ──XREADGROUP──►  FraudeConsumer
                                                  ──XREADGROUP──►  NotificacionesConsumer
                                                  ──XREADGROUP──►  AuditoriaConsumer
```

---

## Cómo funciona

### Productor
`TransferenciaProducer` recibe un evento `TransferenciaCreada` y lo publica
en el stream `banco.transferencias` con `XADD`. Redis le asigna un ID único.

### Redis Stream
Actúa como canal de mensajes. Tiene tres **consumer groups** (uno por consumidor).
Cada grupo recibe todos los mensajes de forma independiente.

### Consumidores
Cada uno corre en su propio hilo, lee con `XREADGROUP` y confirma con `XACK`.

| Consumidor              | Qué hace                                            |
|-------------------------|-----------------------------------------------------|
| `FraudeConsumer`        | Alerta si el monto supera 10.000                    |
| `NotificacionesConsumer`| Simula una notificación al destinatario             |
| `AuditoriaConsumer`     | Guarda el evento en una lista en memoria            |

---

## Estructura del proyecto

```
com.eci.arsw.eda
├── domain/      TransferenciaCreada  (el evento)
├── config/      RedisStreamConfig    (crea el stream y los grupos al arrancar)
├── producer/    TransferenciaProducer
├── consumer/    BaseConsumer, FraudeConsumer, NotificacionesConsumer, AuditoriaConsumer
├── manager/     ConsumerManager      (lanza los hilos)
└── controller/  TransferenciaController
```

---

## Levantar Redis con Docker

```bash
docker run --name redis-eda -p 6379:6379 -d redis:7
```

---

## Correr la aplicación

```bash
mvn spring-boot:run
```

---

## Endpoints

| Método | URL                           | Descripción                          |
|--------|-------------------------------|--------------------------------------|
| POST   | `/api/transferencias`         | Publica un evento (body opcional)    |
| GET    | `/api/transferencias/auditados` | Total guardado por auditoría       |

---

## Probar con curl

**Publicar una transferencia:**
```bash
curl -s -X POST http://localhost:8080/api/transferencias \
  -H "Content-Type: application/json" \
  -d '{"from":"ACC-001","to":"ACC-002","amount":"500.00","currency":"COP"}'
```

**Sin body (usa valores demo):**
```bash
curl -s -X POST http://localhost:8080/api/transferencias
```

**Publicar una transferencia sospechosa (activa alerta de fraude):**
```bash
curl -s -X POST http://localhost:8080/api/transferencias \
  -H "Content-Type: application/json" \
  -d '{"from":"ACC-001","to":"ACC-099","amount":"50000.00","currency":"COP"}'
```

**Ver cuántas transferencias auditó el AuditoriaConsumer:**
```bash
curl http://localhost:8080/api/transferencias/auditados
```

---

## Salida esperada en consola

```
[Productor]       Evento publicado en Redis -> 1750710234567-0
[Fraude]          OK: transferencia abc-123 por 500.00 COP
[Notificaciones]  Enviando notificacion a 'ACC-002': recibiste 500.00 COP de 'ACC-001'
[Auditoria]       Guardado: transferencia abc-123 por 500.00 COP
```
