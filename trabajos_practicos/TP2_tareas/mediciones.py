"""Banco reproducible HTTP local. La variante bloqueante NO se expone en app.py."""
import argparse
import asyncio
import json
import multiprocessing as mp
import platform
import socket
import statistics
import tempfile
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path
from threading import Event

import httpx
import uvicorn
from fastapi.responses import Response


def servidor(sock, modo, listo, detener, cpu_iniciada, directorio):
    import app as modulo
    from modelos import Hashear
    from motor import Motor, hash_iterado

    class MotorMedicion(Motor):
        async def _ejecutar(self, solicitud):
            if isinstance(solicitud, Hashear):
                cpu_iniciada.set()
                if modo == "directo":
                    resultado = hash_iterado(solicitud.texto, solicitud.iteraciones, Event())
                    return {"sha256": resultado, "iteraciones": solicitud.iteraciones}
            return await super()._ejecutar(solicitud)

    # Sustitucion limitada al proceso de medicion, nunca a la API de produccion.
    modulo.Motor = MotorMedicion
    api = modulo.crear_app(workers=4, capacidad=100, datos=directorio)

    @api.get("/fixture")
    async def fixture():
        await asyncio.sleep(0.1)
        return Response(b"x" * 4096)

    async def ejecutar():
        server = uvicorn.Server(uvicorn.Config(api, log_level="error"))
        tarea = asyncio.create_task(server.serve(sockets=[sock]))
        while not server.started:
            if tarea.done():
                await tarea
                raise RuntimeError("No inicio el servidor")
            await asyncio.sleep(0.01)
        listo.set()
        while not detener.is_set():
            await asyncio.sleep(0.01)
        server.should_exit = True
        await tarea

    asyncio.run(ejecutar())


@contextmanager
def instancia(modo="executor"):
    contexto = mp.get_context("spawn")
    listo, detener, cpu = (contexto.Event() for _ in range(3))
    with tempfile.TemporaryDirectory() as datos, socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(128)
        puerto = sock.getsockname()[1]
        proceso = contexto.Process(target=servidor,
                                   args=(sock, modo, listo, detener, cpu, datos))
        proceso.start()
        try:
            if not listo.wait(20):
                raise RuntimeError("Servidor de medicion no disponible")
            with httpx.Client(base_url=f"http://127.0.0.1:{puerto}",
                              timeout=30, trust_env=False) as cliente:
                yield cliente, cpu
        finally:
            detener.set()
            proceso.join(15)
            if proceso.is_alive():
                proceso.terminate()
                proceso.join()
                raise RuntimeError("El servidor de medicion no cerro ordenadamente")
            if proceso.exitcode != 0:
                raise RuntimeError(f"Servidor termino con codigo {proceso.exitcode}")


def crear(cliente, **solicitud):
    respuesta = cliente.post("/tareas", json=solicitud)
    respuesta.raise_for_status()
    return respuesta.json()["id"]


def esperar(cliente, identificadores):
    limite = time.perf_counter() + 30
    pendientes = set(identificadores)
    completadas = {}
    while pendientes and time.perf_counter() < limite:
        respuesta = cliente.get("/tareas?limite=1000")
        respuesta.raise_for_status()
        for tarea in respuesta.json():
            if tarea["id"] not in pendientes:
                continue
            if tarea["estado"] in {"fallida", "cancelada"}:
                raise RuntimeError(tarea)
            if tarea["estado"] == "completada":
                pendientes.remove(tarea["id"])
                completadas[tarea["id"]] = tarea
        if pendientes:
            time.sleep(0.005)
    if pendientes:
        raise TimeoutError(pendientes)
    return completadas


def medir(repeticiones, iteraciones):
    resultado = {
        "fecha_utc": datetime.now(timezone.utc).isoformat(),
        "equipo": {"sistema": platform.platform(), "python": platform.python_version(),
                   "procesador": platform.processor(), "cpu_logicas": mp.cpu_count()},
        "versiones": {n: version(n) for n in ("fastapi", "uvicorn", "httpx", "pydantic")},
        "parametros": {"repeticiones": repeticiones, "iteraciones_hash": iteraciones,
                       "workers": 4, "cola": 100, "descargas": 20,
                       "demora_http_s": 0.1, "bytes_respuesta": 4096},
        "io": [], "cpu": {}, "pool": [],
    }
    with instancia() as (cliente, _):
        url = str(cliente.base_url.join("/fixture"))
        cliente.get("/fixture").raise_for_status()
        for _ in range(repeticiones):
            inicio = time.perf_counter()
            for _ in range(20):
                identificador = crear(cliente, tipo="descargar", urls=[url])
                esperar(cliente, [identificador])
            secuencial = time.perf_counter() - inicio
            inicio = time.perf_counter()
            identificador = crear(cliente, tipo="descargar", urls=[url] * 20)
            tareas = esperar(cliente, [identificador])
            concurrente = time.perf_counter() - inicio
            assert all(r["tamano"] == 4096 for r in tareas[identificador]["resultado"])
            resultado["io"].append({"secuencial_s": secuencial,
                                    "concurrente_s": concurrente,
                                    "speedup": secuencial / concurrente})
        for cantidad in (4, 8, 16, 32):
            for _ in range(repeticiones):
                inicio = time.perf_counter()
                ids = [crear(cliente, tipo="esperar", segundos=0.2) for _ in range(cantidad)]
                tareas = esperar(cliente, ids)
                total = time.perf_counter() - inicio
                latencias = [(datetime.fromisoformat(t["finalizada"]) -
                              datetime.fromisoformat(t["creada"])).total_seconds()
                             for t in tareas.values()]
                resultado["pool"].append({"tareas": cantidad, "lote_s": total,
                                          "latencia_media_s": statistics.mean(latencias),
                                          "latencia_max_s": max(latencias),
                                          "tareas_por_s": cantidad / total})
    for modo in ("directo", "executor"):
        resultado["cpu"][modo] = []
        with instancia(modo) as (cliente, cpu):
            for _ in range(repeticiones):
                cpu.clear()
                identificador = crear(cliente, tipo="hashear", texto="prueba",
                                      iteraciones=iteraciones, timeout=30)
                if not cpu.wait(5):
                    raise RuntimeError("No comenzo el hash")
                inicio = time.perf_counter()
                respuesta = cliente.get("/tareas")
                latencia = time.perf_counter() - inicio
                respuesta.raise_for_status()
                tarea = esperar(cliente, [identificador])[identificador]
                resultado["cpu"][modo].append({"get_s": latencia,
                                              "hash_s": tarea["duracion"]})
    return resultado


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--salida", type=Path, default=Path("mediciones_resultados.json"))
    parser.add_argument("--repeticiones", type=int, default=3)
    parser.add_argument("--iteraciones", type=int, default=2_000_000)
    args = parser.parse_args()
    if args.repeticiones < 1 or not 1 <= args.iteraciones <= 10_000_000:
        parser.error("Repeticiones positivas e iteraciones entre 1 y 10000000")
    datos = medir(args.repeticiones, args.iteraciones)
    args.salida.write_text(json.dumps(datos, indent=2), encoding="utf-8")
    print(json.dumps(datos, indent=2))
