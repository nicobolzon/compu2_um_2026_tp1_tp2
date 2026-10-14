"""Cola acotada, workers y apagado del servicio en un solo event loop."""
import asyncio
import hashlib
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from threading import Event

import httpx

from modelos import Descargar, Esperar, Hashear, SolicitudBase

log = logging.getLogger(__name__)
TERMINALES = {"completada", "fallida", "cancelada"}


def ahora() -> str:
    return datetime.now(timezone.utc).isoformat()


def hash_iterado(texto: str, iteraciones: int, detener: Event) -> str | None:
    datos = texto.encode("utf-8")
    for indice in range(iteraciones):
        if indice % 1024 == 0 and detener.is_set():
            return None
        datos = hashlib.sha256(datos).digest()
    return datos.hex()


@dataclass
class Registro:
    id: int
    solicitud: SolicitudBase
    estado: str = "pendiente"
    creada: str = field(default_factory=ahora)
    iniciada: str | None = None
    finalizada: str | None = None
    inicio: float | None = None
    duracion: float | None = None
    resultado: object = None
    error: str | None = None

    def vista(self) -> dict:
        return {
            "id": self.id, "tipo": self.solicitud.tipo, "estado": self.estado,
            "timeout": self.solicitud.timeout, "creada": self.creada,
            "iniciada": self.iniciada, "finalizada": self.finalizada,
            "duracion": self.duracion, "resultado": self.resultado, "error": self.error,
        }

    def terminar(self, estado: str, error: str | None = None) -> None:
        self.estado = estado
        self.error = error
        self.finalizada = ahora()
        if self.inicio is not None:
            self.duracion = time.perf_counter() - self.inicio


class Motor:
    def __init__(self, workers=4, capacidad=100, apagado=5.0, datos=Path("datos"),
                 transporte=None):
        if workers < 1 or capacidad < 1 or apagado < 0:
            raise ValueError("Workers/capacidad deben ser positivos y apagado no negativo")
        self.cantidad = workers
        self.cola = asyncio.Queue(maxsize=capacidad)
        self.apagado = apagado
        self.datos = Path(datos)
        self.transporte = transporte
        self.registros: dict[int, Registro] = {}
        self.activas: dict[int, asyncio.Task] = {}
        self.workers: list[asyncio.Task] = []
        self.aceptando = False
        self.descargas = asyncio.Semaphore(16)

    async def iniciar(self):
        self.datos.mkdir(parents=True, exist_ok=True)
        anteriores = sorted(self.datos.glob("estado_*.json"))
        if anteriores:
            log.warning("Volcado previo encontrado: %s (no se restaura)", anteriores[-1])
        self.executor = ThreadPoolExecutor(max_workers=self.cantidad,
                                           thread_name_prefix="tp2-hash")
        self.cliente = httpx.AsyncClient(timeout=10, follow_redirects=True,
                                         transport=self.transporte, trust_env=False)
        self.workers = [asyncio.create_task(self._worker(), name=f"tp2-worker-{i}")
                        for i in range(self.cantidad)]
        self.aceptando = True

    def encolar(self, solicitud):
        if not self.aceptando:
            raise RuntimeError("El servicio se esta apagando")
        registro = Registro(id=len(self.registros) + 1, solicitud=solicitud)
        # No hay await entre insertar en la cola y publicar el registro.
        self.cola.put_nowait(registro)
        self.registros[registro.id] = registro
        return registro

    async def cancelar(self, identificador):
        registro = self.registros[identificador]
        if registro.estado in TERMINALES:
            raise ValueError("La tarea ya termino")
        registro.terminar("cancelada", "Cancelada por el cliente")
        activa = self.activas.get(identificador)
        if activa:
            activa.cancel()
            await asyncio.gather(activa, return_exceptions=True)
        return registro

    async def _worker(self):
        while True:
            registro = await self.cola.get()
            try:
                if registro is None:
                    return
                if registro.estado != "pendiente":
                    continue
                registro.estado = "ejecutando"
                registro.iniciada = ahora()
                registro.inicio = time.perf_counter()
                tarea = asyncio.create_task(self._ejecutar(registro.solicitud))
                self.activas[registro.id] = tarea
                try:
                    resultado = await asyncio.wait_for(tarea, registro.solicitud.timeout)
                except TimeoutError:
                    registro.terminar("fallida", "Timeout de ejecucion agotado")
                except asyncio.CancelledError:
                    registro.terminar("cancelada", registro.error or "Apagado del servicio")
                    if asyncio.current_task().cancelling():
                        raise
                except Exception as exc:
                    if isinstance(exc, ExceptionGroup):
                        causas = exc.exceptions
                        detalle = f"{type(causas[0]).__name__}: {causas[0]}"
                        if len(causas) > 1:
                            detalle += f" (+{len(causas) - 1} errores)"
                    else:
                        detalle = f"{type(exc).__name__}: {exc}"
                    registro.terminar("fallida", detalle)
                    log.exception("Fallo la tarea %s", registro.id)
                else:
                    if registro.estado != "cancelada":
                        registro.resultado = resultado
                        registro.terminar("completada")
                finally:
                    self.activas.pop(registro.id, None)
            finally:
                self.cola.task_done()

    async def _ejecutar(self, solicitud):
        if isinstance(solicitud, Esperar):
            await asyncio.sleep(solicitud.segundos)
            return {"segundos": solicitud.segundos}
        if isinstance(solicitud, Hashear):
            detener = Event()
            futuro = asyncio.get_running_loop().run_in_executor(
                self.executor, hash_iterado, solicitud.texto, solicitud.iteraciones, detener
            )
            try:
                resultado = await asyncio.shield(futuro)
            except asyncio.CancelledError:
                # Cancelar el await no mata el hilo: se solicita su salida y se espera.
                detener.set()
                await asyncio.shield(futuro)
                raise
            return {"sha256": resultado, "iteraciones": solicitud.iteraciones}
        if isinstance(solicitud, Descargar):
            async with asyncio.TaskGroup() as grupo:
                tareas = [grupo.create_task(self._descargar(str(url)))
                          for url in solicitud.urls]
            return [tarea.result() for tarea in tareas]
        raise ValueError("Tipo de tarea desconocido")

    async def _descargar(self, url):
        async with self.descargas:
            inicio = time.perf_counter()
            async with self.cliente.stream("GET", url) as respuesta:
                tamano = 0
                async for bloque in respuesta.aiter_bytes():
                    tamano += len(bloque)
                    if tamano > 10 * 1024 * 1024:
                        raise ValueError("Descarga supera el limite de 10 MiB")
                return {"url": url, "codigo": respuesta.status_code,
                        "tamano": tamano, "tiempo": time.perf_counter() - inicio}

    def estadisticas(self):
        por_estado = {estado: 0 for estado in (
            "pendiente", "ejecutando", "completada", "fallida", "cancelada"
        )}
        tiempos = []
        for registro in self.registros.values():
            por_estado[registro.estado] += 1
            if registro.duracion is not None:
                tiempos.append(registro.duracion)
        return {"total": len(self.registros), "por_estado": por_estado,
                "tiempo_promedio": sum(tiempos) / len(tiempos) if tiempos else 0,
                "workers_ocupados": len(self.activas), "workers_total": self.cantidad,
                "aceptando": self.aceptando}

    async def cerrar(self):
        self.aceptando = False
        for registro in self.registros.values():
            if registro.estado == "pendiente":
                registro.terminar("cancelada", "Apagado antes de comenzar")
        try:
            await asyncio.wait_for(self.cola.join(), self.apagado)
        except TimeoutError:
            activas = list(self.activas.values())
            for tarea in activas:
                tarea.cancel()
            await asyncio.gather(*activas, return_exceptions=True)
            await self.cola.join()
        for _ in self.workers:
            await self.cola.put(None)
        await asyncio.gather(*self.workers)
        await self.cliente.aclose()
        self.executor.shutdown(wait=True, cancel_futures=True)
        contenido = {"fecha": ahora(), "tareas": [r.vista() for r in self.registros.values()]}
        await asyncio.to_thread(self._volcar, contenido)

    def _volcar(self, contenido):
        marca = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
        destino = self.datos / f"estado_{marca}.json"
        temporal = destino.with_suffix(".tmp")
        temporal.write_text(json.dumps(contenido, ensure_ascii=False, indent=2), encoding="utf-8")
        temporal.replace(destino)
        log.info("Estado guardado en %s", destino)
