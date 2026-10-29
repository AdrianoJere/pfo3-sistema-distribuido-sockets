# PFO 3: Rediseño como Sistema Distribuido (Cliente-Servidor)

Rediseño del sistema cliente-servidor como una arquitectura distribuida: los
clientes envían tareas por socket a un servidor que las reparte entre varios
procesos worker (cada uno con su propio pool de hilos). Los resultados se
guardan en un almacenamiento compartido y vuelven al cliente que los pidió.

## Diagrama del sistema

![Arquitectura distribuida](diagrama_sistema.svg)

## Qué representa cada componente en el código

Los workers son **procesos independientes** que se comunican con el servidor
por socket, como en una arquitectura distribuida real. Las herramientas de
infraestructura (RabbitMQ, PostgreSQL/S3, Nginx) están **simuladas** con la
librería estándar de Python, para poder ejecutar todo sin instalar ni
configurar servicios externos:

| Componente del diagrama | Herramienta real | Implementación en este repo |
|---|---|---|
| Clientes | App móvil / navegador | `cliente.py`: envía tareas por socket (puerto 5000) |
| Balanceador de carga | Nginx / HAProxy | `servidor.py`: recibe las conexiones y reparte las tareas entre los workers |
| Servidores worker | Varias máquinas, cada una con su pool de hilos | `worker.py`: un proceso por worker, con `ThreadPoolExecutor`; se conecta por socket (puerto 5001) |
| Cola de mensajes | RabbitMQ | `queue.Queue()` en memoria dentro de `servidor.py` |
| Almacenamiento distribuido | PostgreSQL / S3 | SQLite (`resultados.db`), con un `Lock` para escrituras concurrentes |

## Archivos

- `servidor.py`: recibe tareas de los clientes, las encola y las despacha a los workers por socket; guarda los resultados.
- `worker.py`: proceso worker con su pool de hilos. Se pueden iniciar varios.
- `cliente.py`: cliente de consola que envía tareas y muestra el resultado y qué worker/hilo lo procesó.
- `prueba_carga.py`: dispara varias tareas en paralelo y muestra cómo se reparten entre los workers.
- `diagrama_sistema.svg`: diagrama de la arquitectura distribuida.

## Requisitos

Solo Python 3. No hay dependencias externas: todo usa la librería estándar.

## Cómo ejecutarlo

Se necesitan al menos tres terminales, todas en la carpeta del proyecto:

1. **Servidor:**
   ```bash
   python servidor.py
   ```
2. **Uno o más workers** (nombre y cantidad de hilos opcionales):
   ```bash
   python worker.py Worker-A 2
   python worker.py Worker-B 2
   ```
3. **Cliente:**
   ```bash
   python cliente.py
   ```
   Ofrece tres tareas de ejemplo: contar palabras, invertir texto y calcular hash SHA-256.

Para ver el reparto entre workers, con el servidor y los workers ya corriendo:
   ```bash
   python prueba_carga.py 12
   ```

Cada worker simula 0,5 s de trabajo por tarea (`DEMORA_SIMULADA` en `worker.py`) para que el reparto sea visible.

## Cómo funciona

- Los **clientes** hablan con el servidor por el puerto 5000 y envían un único mensaje JSON por conexión.
- Los **workers** se conectan al puerto 5001 y se registran con su nombre y cantidad de hilos.
- El servidor deja cada tarea en la cola y se la manda al primer worker con un hilo libre. Cada worker acepta como máximo tantas tareas en simultáneo como hilos tiene, así la carga se reparte entre todos.
- Cuando un worker termina, devuelve el resultado al servidor, que lo guarda y se lo responde al cliente.
- Si un worker se desconecta con tareas en curso, el servidor las **vuelve a encolar** y las toma otro worker.
- Si no hay ningún worker conectado, el servidor responde con un error en lugar de dejar al cliente esperando.

### Protocolo

**Cliente → servidor:**
```json
{"operacion": "contar_palabras", "texto": "hola mundo"}
```
**Servidor → cliente:**
```json
{"status": "success", "tarea_id": "...", "resultado": "2", "procesado_por": "Worker-A/hilo_0"}
```
Entre servidor y workers los mensajes son JSON, uno por línea.

## Pruebas realizadas

- Dos workers de 2 hilos cada uno recibieron 8 tareas simultáneas de 0,5 s: se repartieron 4 y 4 y terminaron en 1,1 s en total (en serie habrían tardado 4 s).
- Con `prueba_carga.py`, 12 tareas simultáneas sobre un worker de 2 hilos y otro de 4 se repartieron 4 y 8 (proporcional a la capacidad de cada uno) y terminaron en 1,1 s.
- Se terminó (`SIGKILL`) un worker en pleno procesamiento: el servidor re-encoló sus 2 tareas en curso y el otro worker completó las 6, sin que ningún cliente perdiera su respuesta.
- Con ningún worker conectado, el cliente recibe `No hay workers disponibles`.
- Las tres operaciones devuelven el resultado correcto; una operación inexistente o un mensaje que no es JSON devuelven un error controlado sin afectar al servidor.
- Todos los resultados quedan en `resultados.db`, con el worker y el hilo que los procesaron.

## Evidencia de ejecución (Windows)

Servidor, dos workers (Worker-A con 2 hilos y Worker-B con 4) y `prueba_carga.py 12`
corriendo a la vez en cinco terminales. Las 12 tareas se repartieron 4 y 8
(proporcional a los hilos de cada worker) y terminaron en 1,1 s:

![Prueba de reparto entre workers](capturas/prueba_reparto.png)

### Tolerancia a fallos

Con `DEMORA_SIMULADA = 5` en `worker.py` (para tener tiempo de actuar), se lanzaron
12 tareas con `prueba_carga.py` y se cortó Worker-A en pleno procesamiento. El
servidor detectó la desconexión y re-encoló las 2 tareas que tenía en curso
(`Tareas re-encoladas: 2`); Worker-B las completó junto con el resto.

![Tolerancia a fallos](capturas/tolerancia_fallos.png)

## Limitaciones conocidas

- La cola de mensajes es en memoria: si el servidor se reinicia, las tareas pendientes se pierden (RabbitMQ real las persiste en disco).
- El servidor es un único punto de entrada: el balanceador real (Nginx/HAProxy) repartiría conexiones entre varias instancias de servidor.
- Los workers de esta demo corren en la misma máquina (`localhost`); para correrlos en máquinas distintas hay que cambiar `HOST` en `servidor.py` (por `0.0.0.0`) y en `worker.py` (por la dirección del servidor).
