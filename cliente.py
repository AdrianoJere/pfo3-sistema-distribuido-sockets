"""Cliente: envía tareas al servidor distribuido (simulado) y muestra el
resultado procesado por el worker que la haya tomado."""
import json
import socket

HOST = "localhost"
PORT = 5000

OPERACIONES = {
    "1": "contar_palabras",
    "2": "invertir_texto",
    "3": "hash_sha256",
}


def enviar_tarea(operacion, texto):
    """Abre una conexión nueva, envía la tarea en JSON y devuelve la respuesta."""
    cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        cliente.connect((HOST, PORT))
    except ConnectionRefusedError:
        print("No se pudo conectar: ¿está el servidor en ejecución?")
        return None

    try:
        peticion = {"operacion": operacion, "texto": texto}
        cliente.sendall(json.dumps(peticion).encode("utf-8"))
        datos = cliente.recv(4096)
        if not datos:
            print("El servidor cerró la conexión sin responder.")
            return None
        return json.loads(datos.decode("utf-8"))
    except (ConnectionError, OSError) as e:
        print(f"Error de conexión: {e}")
        return None
    finally:
        cliente.close()


def menu():
    while True:
        print("\n1. Contar palabras\n2. Invertir texto\n3. Calcular hash SHA-256\n4. Salir")
        opcion = input("Seleccione una opción: ").strip()

        if opcion == "4":
            print("Saliendo...")
            break
        if opcion not in OPERACIONES:
            print("Opción inválida.")
            continue

        texto = input("Ingrese el texto para la tarea: ")
        respuesta = enviar_tarea(OPERACIONES[opcion], texto)

        if respuesta is None:
            continue
        if respuesta.get("status") == "success":
            print(f"Resultado: {respuesta['resultado']}")
            print(f"Procesado por: {respuesta['procesado_por']} (tarea {respuesta['tarea_id']})")
        else:
            print(f"Error: {respuesta.get('message')}")


if __name__ == "__main__":
    menu()
