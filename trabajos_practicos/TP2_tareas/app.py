"""API de tareas con ciclo de vida y workers asincronos."""
import asyncio
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from modelos import Estado, Solicitud
from motor import Motor


def crear_app(*, workers=None, capacidad=None, apagado=None, datos=None, transporte=None):
    @asynccontextmanager
    async def lifespan(app):
        motor = Motor(
            workers=workers if workers is not None else int(os.getenv("TP2_WORKERS", "4")),
            capacidad=capacidad if capacidad is not None else int(os.getenv("TP2_COLA", "100")),
            apagado=apagado if apagado is not None else float(os.getenv("TP2_APAGADO", "5")),
            datos=datos if datos is not None else os.getenv("TP2_DATOS", "datos"),
            transporte=transporte,
        )
        app.state.motor = motor
        await motor.iniciar()
        try:
            yield
        finally:
            await motor.cerrar()

    app = FastAPI(title="TP2 - Plataforma de tareas", lifespan=lifespan)

    @app.post("/tareas", status_code=201)
    async def crear(nueva: Solicitud):
        try:
            return app.state.motor.encolar(nueva).vista()
        except asyncio.QueueFull:
            raise HTTPException(503, "Cola llena") from None
        except RuntimeError as exc:
            raise HTTPException(503, str(exc)) from exc

    @app.get("/tareas")
    async def listar(estado: Estado | None = None,
                     limite: int = Query(default=100, ge=1, le=1000)):
        registros = app.state.motor.registros.values()
        return [r.vista() for r in registros if estado is None or r.estado == estado][:limite]

    @app.get("/tareas/{tarea_id}")
    async def obtener(tarea_id: int):
        registro = app.state.motor.registros.get(tarea_id)
        if registro is None:
            raise HTTPException(404, "No existe esa tarea")
        return registro.vista()

    @app.delete("/tareas/{tarea_id}")
    async def cancelar(tarea_id: int):
        try:
            return (await app.state.motor.cancelar(tarea_id)).vista()
        except KeyError:
            raise HTTPException(404, "No existe esa tarea") from None
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.get("/estadisticas")
    async def estadisticas():
        return app.state.motor.estadisticas()

    return app


app = crear_app()
