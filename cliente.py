import socket
import json
import threading
import time

HOST = "localhost"
PORT = 9000

def escuchar_respuestas(sock, activo):
    buffer = ""
    while activo["corriendo"]:
        try:
            sock.settimeout(0.5)
            datos = sock.recv(4096).decode("utf-8")
            if not datos:
                break
            buffer += datos
            while "\n" in buffer:
                linea, buffer = buffer.split("\n", 1)
                linea = linea.strip()
                if not linea:
                    continue

                try:
                    respuesta = json.loads(linea)

                    if respuesta.get("estado") == "encolada":
                        print(f"\n  [ENCOLADA] {respuesta['mensaje']}")
                    else:
                        print(f"\n  [RESULTADO] Tarea #{respuesta['tarea_id']} "
                              f"— {respuesta['resultado']}")
                        print(f"             Worker: {respuesta['worker']} "
                              f"| Hora: {respuesta['timestamp']}")

                except json.JSONDecodeError:
                    print(f"\n  [SERVIDOR] {linea}")

        except socket.timeout:
            continue
        except Exception:
            break


def iniciar_cliente():
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((HOST, PORT))
        print(f"[CLIENTE] Conectado al servidor {HOST}:{PORT}")
        print("[CLIENTE] Escribí tareas para enviar. Ingresá 'FIN' para salir.\n")

        activo = {"corriendo": True}
        hilo_respuestas = threading.Thread(
            target=escuchar_respuestas,
            args=(sock, activo),
            daemon=True
        )
        hilo_respuestas.start()

        while True:
            tarea = input("\nTarea a enviar: ").strip()

            if not tarea:
                print("[!] La tarea no puede estar vacía.")
                continue

            if tarea.upper() == "FIN":
                sock.send("FIN\n".encode("utf-8"))
                time.sleep(0.5) 
                break

            sock.send((tarea + "\n").encode("utf-8"))
            print(f"[CLIENTE] Tarea enviada: '{tarea}'")

            time.sleep(2.5)

    except ConnectionRefusedError:
        print(f"[ERROR] No se pudo conectar a {HOST}:{PORT}. ¿El servidor está corriendo?")
    except KeyboardInterrupt:
        print("\n[CLIENTE] Interrumpido por el usuario.")
    finally:
        activo["corriendo"] = False
        sock.close()
        print("[CLIENTE] Conexión cerrada.")

if __name__ == "__main__":
    print("=" * 55)
    print("  CLIENTE DISTRIBUIDO - PFO3 Redes 2026")
    print("=" * 55)
    iniciar_cliente()