"""Servidor distribuido: recibe tareas de los clientes por socket (puerto 5000),
las pone en una cola (simula RabbitMQ) y las despacha por socket a procesos
worker independientes (puerto 5001, ver worker.py). Cuando un worker devuelve
el resultado, se guarda en SQLite (simula PostgreSQL/S3) y se responde al cliente."""
import json
import queue
import socket
import sqlite3
import sys
import threading
import uuid
from datetime import datetime

HOST = "localhost"
PORT_CLIENTES = 5000
PORT_WORKERS = 5001
DB_NAME = "resultados.db"

# Cola de tareas pendientes de despacho: simula la cola de RabbitMQ.
cola_tareas = queue.Queue()

# tarea_id -> conexión del cliente que está esperando el resultado
pendientes = {}
lock_pendientes = threading.Lock()

# nombre -> info del worker conectado (para saber si hay alguno disponible)
workers = {}
lock_workers = threading.Lock()

# Lock para que dos hilos no escriban en la DB al mismo tiempo (sección crítica)
lock_db = threading.Lock()


def inicializar_db():
    """Crea el almacenamiento distribuido (simulado con SQLite) si no existe."""
    try:
        conexion = sqlite3.connect(DB_NAME)
        conexion.execute(
            """CREATE TABLE IF NOT EXISTS resultados (
                   id TEXT PRIMARY KEY,
                   operacion TEXT NOT NULL,
                   entrada TEXT NOT NULL,
                   resultado TEXT NOT NULL,
                   procesado_por TEXT NOT NULL,
                   fecha TEXT NOT NULL
               )"""
        )
        conexion.commit()
        conexion.close()
    except sqlite3.Error as e:
        print(f"[ERROR] No se pudo acceder al almacenamiento: {e}")
        sys.exit(1)


def guardar_resultado(tarea, resultado, procesado_por):
    """Persiste el resultado en el almacenamiento distribuido (simulado)."""
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with lock_db:
        conexion = sqlite3.connect(DB_NAME)
        conexion.execute(
            "INSERT INTO resultados (id, operacion, entrada, resultado, procesado_por, fecha) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (tarea["id"], tarea["operacion"], tarea["texto"], resultado, procesado_por, fecha),
        )
        conexion.commit()
        conexion.close()


def crear_socket_servidor(puerto):
    """Crea y configura un socket de escucha en el puerto indicado."""
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # En Windows no se activa SO_REUSEADDR: ahí permite abrir dos servidores
    # en el mismo puerto y no se detectaría el error de "puerto ocupado".
    if sys.platform != "win32":
        servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        servidor.bind((HOST, puerto))
    except OSError as e:
        print(f"[ERROR] No se pudo usar el puerto {puerto} (¿ocupado?): {e}")
        sys.exit(1)
    servidor.listen(50)
    servidor.settimeout(1.0)  # permite que Ctrl+C funcione también en Windows
    return servidor


def responder_cliente(conexion, respuesta):
    """Envía la respuesta JSON al cliente y cierra su conexión."""
    try:
        conexion.sendall(json.dumps(respuesta).encode("utf-8"))
    except (ConnectionError, OSError):
        print("[AVISO] El cliente se desconectó antes de recibir la respuesta")
    finally:
        conexion.close()


def finalizar_tarea(mensaje, nombre_worker):
    """Procesa el resultado devuelto por un worker y responde al cliente."""
    with lock_pendientes:
        entrada = pendientes.pop(mensaje.get("tarea_id"), None)
    if entrada is None:
        return  # tarea desconocida o ya respondida
    tarea, conexion_cliente = entrada

    if mensaje.get("status") == "success":
        procesado_por = mensaje.get("procesado_por", nombre_worker)
        try:
            guardar_resultado(tarea, mensaje["resultado"], procesado_por)
            respuesta = {
                "status": "success",
                "tarea_id": tarea["id"],
                "resultado": mensaje["resultado"],
                "procesado_por": procesado_por,
            }
        except sqlite3.Error as e:
            respuesta = {"status": "error", "message": f"No se pudo guardar el resultado: {e}"}
    else:
        respuesta = {"status": "error", "message": mensaje.get("message", "Error en el worker")}
    responder_cliente(conexion_cliente, respuesta)


def enviar_tareas_al_worker(conexion, nombre, semaforo, estado):
    """Hilo emisor: saca tareas de la cola y las manda al worker por socket.
    El semáforo limita las tareas en vuelo a la cantidad de hilos del worker,
    así la carga se reparte entre workers en vez de acapararla uno solo."""
    while not estado["cerrado"]:
        if not semaforo.acquire(timeout=1):
            continue
        try:
            tarea = cola_tareas.get(timeout=1)
        except queue.Empty:
            semaforo.release()
            continue

        with estado["lock"]:
            if estado["cerrado"]:  # el worker se cayó mientras esperábamos
                cola_tareas.put(tarea)
                return
            estado["en_vuelo"][tarea["id"]] = tarea
        try:
            conexion.sendall((json.dumps(tarea) + "\n").encode("utf-8"))
        except OSError:
            # Se despierta al hilo receptor: él re-encola lo que quedó en vuelo.
            try:
                conexion.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            return


def atender_worker(conexion, direccion):
    """Registra un worker y recibe sus resultados. Si el worker se desconecta,
    las tareas que tenía en vuelo vuelven a la cola para otro worker."""
    lector = conexion.makefile("r", encoding="utf-8")
    try:
        registro = json.loads(lector.readline())
        nombre = str(registro["nombre"])
        capacidad = max(1, int(registro.get("hilos", 1)))
    except (ValueError, KeyError, TypeError):
        print(f"[AVISO] Registro de worker inválido desde {direccion[0]}")
        conexion.close()
        return

    estado = {"cerrado": False, "en_vuelo": {}, "lock": threading.Lock()}
    semaforo = threading.Semaphore(capacidad)
    with lock_workers:
        workers[nombre] = capacidad
    print(f"[INFO] {nombre} conectado ({capacidad} hilos). Workers activos: {len(workers)}")

    threading.Thread(
        target=enviar_tareas_al_worker, args=(conexion, nombre, semaforo, estado), daemon=True
    ).start()

    try:
        for linea in lector:
            try:
                mensaje = json.loads(linea)
            except json.JSONDecodeError:
                continue
            with estado["lock"]:
                estado["en_vuelo"].pop(mensaje.get("tarea_id"), None)
            semaforo.release()
            finalizar_tarea(mensaje, nombre)
    except (ConnectionError, OSError):
        pass
    finally:
        with estado["lock"]:
            estado["cerrado"] = True
            huerfanas = list(estado["en_vuelo"].values())
            estado["en_vuelo"].clear()
        for tarea in huerfanas:
            cola_tareas.put(tarea)
        with lock_workers:
            workers.pop(nombre, None)
        conexion.close()
        print(f"[INFO] {nombre} desconectado. Tareas re-encoladas: {len(huerfanas)}. "
              f"Workers activos: {len(workers)}")


def aceptar_workers(servidor_workers):
    """Hilo que acepta las conexiones de los procesos worker."""
    while True:
        try:
            conexion, direccion = servidor_workers.accept()
        except socket.timeout:
            continue
        except OSError:
            return
        conexion.settimeout(None)
        threading.Thread(target=atender_worker, args=(conexion, direccion), daemon=True).start()


def recibir_tarea(conexion):
    """Lee y parsea el JSON enviado por el cliente. Devuelve None si es inválido."""
    try:
        datos = conexion.recv(4096)
        return json.loads(datos.decode("utf-8")) if datos else None
    except (json.JSONDecodeError, UnicodeDecodeError, OSError):
        return None


def aceptar_clientes(servidor_clientes):
    """Acepta clientes y encola cada tarea recibida (rol de dispatcher/balanceador)."""
    while True:
        try:
            conexion, direccion = servidor_clientes.accept()
        except socket.timeout:
            continue
        print(f"[INFO] Tarea entrante desde {direccion[0]}")

        peticion = recibir_tarea(conexion)
        if not isinstance(peticion, dict) or "operacion" not in peticion or "texto" not in peticion:
            responder_cliente(
                conexion, {"status": "error", "message": "JSON inválido. Se espera {operacion, texto}"}
            )
            continue

        with lock_workers:
            hay_workers = bool(workers)
        if not hay_workers:
            responder_cliente(
                conexion, {"status": "error", "message": "No hay workers disponibles. Iniciá worker.py"}
            )
            continue

        tarea = {
            "id": str(uuid.uuid4()),
            "operacion": peticion["operacion"],
            "texto": peticion["texto"],
        }
        with lock_pendientes:
            pendientes[tarea["id"]] = (tarea, conexion)
        cola_tareas.put(tarea)  # equivale a publicar un mensaje en RabbitMQ


def main():
    inicializar_db()
    servidor_clientes = crear_socket_servidor(PORT_CLIENTES)
    servidor_workers = crear_socket_servidor(PORT_WORKERS)
    print(f"[INFO] Clientes en {HOST}:{PORT_CLIENTES} | Workers en {HOST}:{PORT_WORKERS}")
    threading.Thread(target=aceptar_workers, args=(servidor_workers,), daemon=True).start()
    try:
        aceptar_clientes(servidor_clientes)
    except KeyboardInterrupt:
        print("\n[INFO] Servidor detenido")
    finally:
        servidor_clientes.close()
        servidor_workers.close()


if __name__ == "__main__":
    main()
