"""Prueba de carga: envía varias tareas en paralelo (una conexión por tarea) y
muestra qué worker e hilo procesó cada una. Sirve para ver el reparto entre
workers.  Uso:  python prueba_carga.py [cantidad_de_tareas]"""
import json
import socket
import sys
import threading
import time

HOST = "localhost"
PORT = 5000
resultados = {}


def enviar_tarea(numero):
    cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        cliente.connect((HOST, PORT))
        peticion = {"operacion": "contar_palabras", "texto": f"tarea numero {numero} de la prueba"}
        cliente.sendall(json.dumps(peticion).encode("utf-8"))
        resultados[numero] = json.loads(cliente.recv(4096).decode("utf-8"))
    except (ConnectionError, OSError, ValueError) as e:
        resultados[numero] = {"status": "error", "message": str(e)}
    finally:
        cliente.close()


def main():
    try:
        cantidad = int(sys.argv[1]) if len(sys.argv) > 1 else 12
    except ValueError:
        print("La cantidad de tareas debe ser un número entero")
        return

    inicio = time.time()
    hilos = [threading.Thread(target=enviar_tarea, args=(n,)) for n in range(1, cantidad + 1)]
    for h in hilos:
        h.start()
    for h in hilos:
        h.join()
    total = time.time() - inicio

    por_worker = {}
    for numero in sorted(resultados):
        r = resultados[numero]
        if r.get("status") == "success":
            worker = r["procesado_por"].split("/")[0]
            por_worker[worker] = por_worker.get(worker, 0) + 1
            print(f"Tarea {numero:>2}: resultado {r['resultado']} -> {r['procesado_por']}")
        else:
            print(f"Tarea {numero:>2}: ERROR {r.get('message')}")

    print(f"\n{cantidad} tareas en {total:.1f} s")
    for worker, n in sorted(por_worker.items()):
        print(f"  {worker}: {n} tareas")


if __name__ == "__main__":
    main()
