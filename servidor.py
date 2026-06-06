import socket
import threading
import queue
import time
import datetime
import json

HOST = "localhost"
PORT = 9000
MAX_WORKERS = 4        
TASK_QUEUE = queue.Queue()   
RESULTS = {}                 
RESULTS_LOCK = threading.Lock()  
task_counter = 0
task_counter_lock = threading.Lock()

def procesar_tarea(tarea_id, contenido, worker_id):
    print(f"[WORKER-{worker_id}] Procesando tarea #{tarea_id}: '{contenido}'")

    time.sleep(1)

    resultado = {
        "tarea_id": tarea_id,
        "contenido": contenido,
        "resultado": f"Tarea '{contenido}' completada",
        "worker": worker_id,
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    with RESULTS_LOCK:
        RESULTS[tarea_id] = resultado

    print(f"[WORKER-{worker_id}] Tarea #{tarea_id} finalizada y guardada.")
    return resultado

def worker_loop(worker_id):
    print(f"[WORKER-{worker_id}] Listo y esperando tareas...")

    while True:
        try:
            tarea = TASK_QUEUE.get(timeout=1)

            if tarea is None:
                print(f"[WORKER-{worker_id}] Recibió señal de cierre.")
                break

            tarea_id = tarea["id"]
            contenido = tarea["contenido"]
            conn_cliente = tarea["conn"]

            resultado = procesar_tarea(tarea_id, contenido, worker_id)

            try:
                respuesta = json.dumps(resultado) + "\n"
                conn_cliente.send(respuesta.encode("utf-8"))
            except Exception as e:
                print(f"[WORKER-{worker_id}] Error enviando respuesta: {e}")

            TASK_QUEUE.task_done()

        except queue.Empty:
            continue
        except Exception as e:
            print(f"[WORKER-{worker_id}] Error inesperado: {e}")


def manejar_cliente(conn, addr):
    print(f"\n[SERVIDOR] Cliente conectado: {addr[0]}:{addr[1]}")

    try:
        while True:
            datos = conn.recv(1024)
            if not datos:
                print(f"[SERVIDOR] Cliente {addr} desconectado.")
                break

            mensaje = datos.decode("utf-8").strip()

            if mensaje.upper() == "FIN":
                conn.send("Conexión cerrada. ¡Hasta luego!\n".encode("utf-8"))
                break

            global task_counter
            with task_counter_lock:
                task_counter += 1
                tarea_id = task_counter

            print(f"[SERVIDOR] Tarea #{tarea_id} recibida de {addr}: '{mensaje}'")

            confirmacion = json.dumps({
                "estado": "encolada",
                "tarea_id": tarea_id,
                "mensaje": f"Tarea #{tarea_id} recibida y encolada para procesamiento"
            }) + "\n"
            conn.send(confirmacion.encode("utf-8"))

            TASK_QUEUE.put({
                "id": tarea_id,
                "contenido": mensaje,
                "conn": conn,
                "timestamp": datetime.datetime.now().isoformat()
            })

    except ConnectionResetError:
        print(f"[SERVIDOR] Cliente {addr} interrumpió la conexión.")
    except Exception as e:
        print(f"[SERVIDOR] Error con cliente {addr}: {e}")
    finally:
        conn.close()


def iniciar_workers():
    workers = []
    for i in range(1, MAX_WORKERS + 1):
        hilo = threading.Thread(
            target=worker_loop,
            args=(i,),
            daemon=True 
        )
        hilo.start()
        workers.append(hilo)
        print(f"[POOL] Worker {i} iniciado.")
    return workers


def iniciar_servidor():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        srv.bind((HOST, PORT))
        srv.listen(10)  
        print(f"[SERVIDOR] Escuchando en {HOST}:{PORT}")
        print(f"[SERVIDOR] Pool de {MAX_WORKERS} workers activo.")
        print("[SERVIDOR] Esperando clientes...\n")

        while True:
            conn, addr = srv.accept()
            hilo_cliente = threading.Thread(
                target=manejar_cliente,
                args=(conn, addr),
                daemon=True
            )
            hilo_cliente.start()

    except OSError as e:
        if "already in use" in str(e) or e.errno in (98, 10048):
            print(f"[ERROR] Puerto {PORT} ocupado. Cerrá el proceso anterior.")
        else:
            print(f"[ERROR] {e}")
    except KeyboardInterrupt:
        print("\n[SERVIDOR] Apagando servidor...")
        for _ in range(MAX_WORKERS):
            TASK_QUEUE.put(None)
    finally:
        srv.close()
        print("[SERVIDOR] Socket cerrado.")

if __name__ == "__main__":
    print("=" * 55)
    print("  SERVIDOR DISTRIBUIDO - PFO3 Redes 2026")
    print(f"  Workers: {MAX_WORKERS} | Puerto: {PORT}")
    print("=" * 55)

    iniciar_workers()
    iniciar_servidor()