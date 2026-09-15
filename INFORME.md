# Informe - Trabajo Práctico: Middlewares Orientados a Mensajes (MOM)

## Resumen

Se implementó la interfaz de middleware provista en `/python/src/common/middleware/middleware.py` sobre RabbitMQ, completando las clases `MessageMiddlewareQueueRabbitMQ` y `MessageMiddlewareExchangeRabbitMQ` en `/python/src/common/middleware/middleware_rabbitmq.py` usando la librería `pika`.

Para el desarrollo se leyó la documentación de RabbitMQ y se siguieron los tutoriales en lenguaje python. Se tomaron como base para entender los conceptos de colas, exchanges con routing keys, bindings y confirmaciones (ACK/NACK), y luego se aplicaron sobre las interfaces del enunciado.

## Decisiones de diseño

### Colas (`MessageMiddlewareQueueRabbitMQ`)

- La cola se declara como `durable`, de forma consistente entre todos los productores/consumidores que comparten el mismo nombre. Se optó por una cola clásica básica de RabbitMQ y se evitaron funcionalidades específicas del broker (como quorum queues), manteniendo la solución sobre los primitivos básicos que cualquier MOM expone.
- `close()` se limita a cerrar el canal y la conexión: **no se elimina la cola**, ya que es un recurso compartido y otros clientes conectados al mismo nombre se verían afectados si un objeto la borrara.

### Exchanges (`MessageMiddlewareExchangeRabbitMQ`)

- El exchange se declara como `direct`, de forma consistente en todas las instancias, ya que el patrón de pruebas utiliza bindings por routing key.
- La cola anónima y exclusiva del consumidor (nombre generado por el servidor) se crea recién en `start_consuming`, y ahí se bindea a cada routing key recibida en la inicialización. Esto evita que los productores (que usan la misma clase) creen colas que no consumen ningún mensaje.
- Si falla la creación/bindeo de la cola exclusiva, se intenta borrarla antes de devolver el error, para no dejar colas huérfanas. No es necesario borrarla en `close()`: por ser exclusiva, el broker la elimina automáticamente al cerrarse la conexión.
- `send()` publica al exchange usando la routing key con la que se inicializó el middleware (`routing_keys[0]`), que en los casos de prueba es la del productor. Si la instancia se inicializó sin routing keys, `send()` eleva `MessageMiddlewareMessageError` como error interno del middleware.

### Adaptación del callback (ACK/NACK)

- Pika invoca el callback de consumo con los argumentos `(ch, method, properties, body)`, mientras que la interfaz definida en `middleware.py` espera `(message, ack, nack)`.
- Se implementó `_wrap_callback`, una función que traduce entre ambas, expone el body como `message` y construye closures de `ack`/`nack` que capturan el `delivery_tag` del mensaje recibido, de modo que cada ACK/NACK corresponda al mensaje correcto.

### Ciclo de consumo (Start -> Stop -> Start)

- Se implementó un flag de estado (`_is_consuming`) que impide llamar a `start_consuming` dos veces sin un `stop_consuming` en el medio. Se garantiza el ciclo `start_consuming -> stop_consuming -> start_consuming -> ...`.
- El flag se resetea siempre con `finally`, tanto en la salida normal (por `stop_consuming` desde el callback) como ante errores.
- No se utilizaron mecanismos adicionales de sincronización: siguiendo el criterio indicado en las consultas en el foro, se asume una instancia de middleware por hilo/proceso y no se comparten canales ni conexiones.

### Manejo de recursos

- En los constructores se usan variables locales y se asignan a `self` recién al final. Si falla un paso intermedio (conexión, canal o declaración), se liberan los recursos ya creados con `_close_channel_connection` antes de propagar el error.
- `close()` delega en `_close_channel_connection`, que se ocupa de cerrar el canal y luego la conexión con `try/finally`. Esto garantiza que se intente cerrar la conexión aunque falle el cierre del canal, mapeando cualquier error a `MessageMiddlewareCloseError`.

### Reutilización de código

- Siguiendo las consultas de otros compañeros en el foro, se evaluó generar una clase base para la funcionalidad común entre cola y exchange (que solo difieren en `__init__` y `send`). 
También se evaluó usar context managers para unificar el manejo de errores. Se descartaron ambas opciones teniendo en cuenta la interpretación de que el objetivo del trabajo es familiarizarse con los MOM y que estas abstracciones adicionales no eran el foco principal en esta instancia de la cursada.
- Se decidió utilizar helpers para los tres patrones que se repetían en ambas clases: `_wrap_callback` (adaptación del callback de pika), `_handle_pika_error` (traducción de errores de pika) y `_close_channel_connection` (cierre de recursos).

## Manejo de errores

- La traducción de excepciones de pika a los errores del contrato definidos en `middleware.py` se centraliza en `_handle_pika_error`:
  - `AMQPConnectionError` (pérdida de conexión) → `MessageMiddlewareDisconnectedError`.
  - Resto de errores de pika (`AMQPError`) → `MessageMiddlewareMessageError`.
- `close()` es el caso especial: no reutiliza `_handle_pika_error` porque cualquier error interno de desconexión debe elevar `MessageMiddlewareCloseError`, de forma consistente en ambas clases.