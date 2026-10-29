"""Worker: proceso independiente que se conecta al servidor por socket, recibe
tareas y las procesa con su propio pool de hilos. Se pueden iniciar varios
workers en terminales distintas:

    python worker.py Worker-A
    python worker.py Worker-B 2     (nombre y cantidad de hilos)
"""
import hashlib
import json
import socket
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

HOST = "localhost"
PORT_WORKERS = 5001
HILOS_POR_DEFECTO = 4
DEMORA_SIMULADA = 5  # segundos: simula que cada tarea tarda, para ver el reparto

# Varios hilos del pool responden por el mismo socket: hace falta un lock de envío
lock_envio = threading.Lock()


def procesar_tarea(operacion, texto):
    """Ejecuta la operación solicitada. Lanza ValueError si la operación no existe."""
    time.sleep(DEMORA_SIMULADA)
    if operacion == "contar_palabras":
        return str(len(texto.split()))
    elif operacion == "invertir_texto":
        return texto[::-1]
    elif operacion == "hash_sha256":
        return hashlib.sha256(texto.encode("utf-8")).hexdigest()
    else:
        raise ValueError(f"Operación no soportada: {operacion}")


def enviar(conexion, mensaje):
    """Envía un mensaje JSON (una línea) al servidor, de forma segura entre hilos."""
    with lock_envio:
        conexion.sendall((json.dumps(mensaje) + "\n").encode("utf-8"))


def atender_tarea(conexion, nombre, tarea):
    """Tarea del pool de hilos: procesa y devuelve el resultado al servidor."""
    hilo = threading.current_thread().name
    try:
        resultado = procesar_tarea(tarea["operacion"], tarea["texto"])
        mensaje = {
            "tarea_id": tarea["id"],
            "status": "success",
            "resultado": resultado,
            "procesado_por": f"{nombre}/{hilo}",
        }
    except (KeyError, ValueError) as e:
        mensaje = {"tarea_id": tarea.get("id"), "status": "error", "message": str(e)}
    print(f"[{nombre}/{hilo}] Tarea {str(tarea.get('id'))[:8]} -> {mensaje['status']}")
    try:
        enviar(conexion, mensaje)
    except OSError:
        print(f"[{nombre}] No se pudo enviar el resultado: servidor desconectado")


def main():
    nombre = sys.argv[1] if len(sys.argv) > 1 else "Worker-1"
    try:
        hilos = int(sys.argv[2]) if len(sys.argv) > 2 else HILOS_POR_DEFECTO
    except ValueError:
        print("La cantidad de hilos debe ser un número entero")
        return

    conexion = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        conexion.connect((HOST, PORT_WORKERS))
    except ConnectionRefusedError:
        print("No se pudo conectar: ¿está el servidor en ejecución?")
        return

    enviar(conexion, {"nombre": nombre, "hilos": hilos})
    print(f"[{nombre}] Conectado al servidor con un pool de {hilos} hilos")

    lector = conexion.makefile("r", encoding="utf-8")
    # El nombre de cada hilo (hilo_0, hilo_1...) aparece en el resultado: Worker-A/hilo_0
    with ThreadPoolExecutor(max_workers=hilos, thread_name_prefix="hilo") as pool:
        try:
            for linea in lector:
                try:
                    tarea = json.loads(linea)
                except json.JSONDecodeError:
                    continue
                pool.submit(atender_tarea, conexion, nombre, tarea)
        except KeyboardInterrupt:
            print(f"\n[{nombre}] Worker detenido")
        except (ConnectionError, OSError):
            pass
    print(f"[{nombre}] Desconectado del servidor")
    conexion.close()


if __name__ == "__main__":
    main()
