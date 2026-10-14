import asyncio
import hashlib
import json
import tempfile
import time
import unittest
from pathlib import Path

import httpx
from fastapi.testclient import TestClient

from app import crear_app
from modelos import Esperar, Hashear
from motor import Motor


class ApiTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.client = TestClient(crear_app(workers=1, capacidad=2, apagado=0.01,
                                          datos=self.tmp.name))
        self.client.__enter__()

    def tearDown(self):
        self.client.__exit__(None, None, None)
        self.tmp.cleanup()

    def esperar_estado(self, identificador, estado):
        limite = time.monotonic() + 3
        while time.monotonic() < limite:
            tarea = self.client.get(f"/tareas/{identificador}").json()
            if tarea["estado"] == estado:
                return tarea
            time.sleep(0.01)
        self.fail(f"No llego a {estado}: {tarea}")

    def crear(self, **payload):
        respuesta = self.client.post("/tareas", json=payload)
        self.assertEqual(respuesta.status_code, 201, respuesta.text)
        return respuesta.json()["id"]

    def test_espera_y_estadisticas(self):
        identificador = self.crear(tipo="esperar", segundos=0.01)
        self.esperar_estado(identificador, "completada")
        stats = self.client.get("/estadisticas").json()
        self.assertEqual(stats["por_estado"]["completada"], 1)
        self.assertGreater(stats["tiempo_promedio"], 0)
        self.assertEqual(len(self.client.get("/tareas?estado=completada").json()), 1)
        self.assertEqual(self.client.delete(f"/tareas/{identificador}").status_code, 409)

    def test_hash(self):
        identificador = self.crear(tipo="hashear", texto="hola", iteraciones=2)
        tarea = self.esperar_estado(identificador, "completada")
        esperado = hashlib.sha256(hashlib.sha256(b"hola").digest()).hexdigest()
        self.assertEqual(tarea["resultado"]["sha256"], esperado)

    def test_timeout_y_worker_sigue(self):
        identificador = self.crear(tipo="esperar", segundos=10, timeout=0.02)
        tarea = self.esperar_estado(identificador, "fallida")
        self.assertIn("Timeout", tarea["error"])
        siguiente = self.crear(tipo="esperar", segundos=0)
        self.esperar_estado(siguiente, "completada")

    def test_cancelacion_cola_saturacion(self):
        activa = self.crear(tipo="esperar", segundos=10)
        self.esperar_estado(activa, "ejecutando")
        pendiente = self.crear(tipo="esperar", segundos=10)
        self.crear(tipo="esperar", segundos=10)
        self.assertEqual(self.client.post("/tareas", json={"tipo":"esperar","segundos":0}).status_code, 503)
        self.assertEqual(self.client.delete(f"/tareas/{pendiente}").json()["estado"], "cancelada")
        self.assertEqual(self.client.delete(f"/tareas/{activa}").json()["estado"], "cancelada")
        self.assertEqual(self.client.get(f"/tareas/{pendiente}").status_code, 200)

    def test_validacion(self):
        for payload in [{"tipo":"otro"}, {"tipo":"esperar","segundos":-1},
                        {"tipo":"hashear","texto":"x","iteraciones":0},
                        {"tipo":"descargar","urls":["file:///etc/passwd"]}]:
            self.assertEqual(self.client.post("/tareas", json=payload).status_code, 422)
        self.assertEqual(self.client.get("/tareas/999").status_code, 404)
        self.assertEqual(self.client.delete("/tareas/999").status_code, 404)


class MotorTests(unittest.IsolatedAsyncioTestCase):
    async def test_cancelar_descarga_cierra_stream(self):
        from modelos import Descargar
        iniciada = asyncio.Event()
        cerrada = asyncio.Event()

        class Flujo(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield b"inicio"
                iniciada.set()
                await asyncio.sleep(60)

            async def aclose(self):
                cerrada.set()

        transporte = httpx.MockTransport(lambda request: httpx.Response(200, stream=Flujo()))
        with tempfile.TemporaryDirectory() as tmp:
            motor = Motor(datos=tmp, transporte=transporte)
            await motor.iniciar()
            try:
                tarea = motor.encolar(Descargar(tipo="descargar", urls=["https://test.local/a"]))
                await asyncio.wait_for(iniciada.wait(), 2)
                await motor.cancelar(tarea.id)
                await asyncio.wait_for(motor.cola.join(), 2)
                self.assertTrue(cerrada.is_set())
                self.assertEqual(tarea.estado, "cancelada")
                self.assertEqual(motor.activas, {})
            finally:
                await motor.cerrar()

    async def test_timeout_hash_libera_worker(self):
        with tempfile.TemporaryDirectory() as tmp:
            motor = Motor(workers=1, datos=tmp)
            await motor.iniciar()
            try:
                tarea = motor.encolar(Hashear(tipo="hashear", texto="x",
                                             iteraciones=10_000_000, timeout=0.02))
                siguiente = motor.encolar(Esperar(tipo="esperar", segundos=0))
                await asyncio.wait_for(motor.cola.join(), 5)
                self.assertEqual(tarea.estado, "fallida")
                self.assertIn("Timeout", tarea.error)
                self.assertEqual(siguiente.estado, "completada")
                self.assertEqual(motor.activas, {})
            finally:
                await motor.cerrar()

    async def test_apagado_deja_completar_tarea_corta(self):
        with tempfile.TemporaryDirectory() as tmp:
            motor = Motor(datos=tmp, apagado=1)
            await motor.iniciar()
            tarea = motor.encolar(Esperar(tipo="esperar", segundos=0.05))
            for _ in range(100):
                if tarea.estado == "ejecutando":
                    break
                await asyncio.sleep(0.001)
            self.assertEqual(tarea.estado, "ejecutando")
            await motor.cerrar()
            self.assertEqual(tarea.estado, "completada")

    async def test_descargas_concurrentes_y_fallo(self):
        from modelos import Descargar
        activas = 0
        maximo = 0

        async def respuesta(request):
            nonlocal activas, maximo
            if request.url.path == "/error":
                raise httpx.ConnectError("fallo simulado")
            activas += 1
            maximo = max(maximo, activas)
            try:
                await asyncio.sleep(0.03)
                return httpx.Response(200, content=b"abc")
            finally:
                activas -= 1

        with tempfile.TemporaryDirectory() as tmp:
            motor = Motor(datos=tmp, transporte=httpx.MockTransport(respuesta))
            await motor.iniciar()
            try:
                tarea = motor.encolar(Descargar(tipo="descargar", urls=["https://test.local/a"] * 3))
                await asyncio.wait_for(motor.cola.join(), 3)
                self.assertEqual(tarea.estado, "completada")
                self.assertEqual(maximo, 3)
                self.assertEqual(tarea.resultado[0]["tamano"], 3)
                mala = motor.encolar(Descargar(tipo="descargar", urls=["https://test.local/error"]))
                with self.assertLogs("motor", level="ERROR"):
                    await asyncio.wait_for(motor.cola.join(), 3)
                self.assertEqual(mala.estado, "fallida")
                self.assertIn("fallo simulado", mala.error)
                self.assertIn("ConnectError", mala.error)
                buena = motor.encolar(Esperar(tipo="esperar", segundos=0))
                await asyncio.wait_for(motor.cola.join(), 3)
                self.assertEqual(buena.estado, "completada")
            finally:
                await motor.cerrar()

    async def test_apagado_hash_y_volcado(self):
        with tempfile.TemporaryDirectory() as tmp:
            motor = Motor(workers=1, apagado=0.01, datos=tmp)
            await motor.iniciar()
            tarea = motor.encolar(Hashear(tipo="hashear", texto="x", iteraciones=10_000_000))
            await asyncio.sleep(0.02)
            pendiente = motor.encolar(Esperar(tipo="esperar", segundos=10))
            await asyncio.wait_for(motor.cerrar(), 3)
            self.assertEqual(tarea.estado, "cancelada")
            self.assertEqual(pendiente.estado, "cancelada")
            self.assertTrue(all(w.done() for w in motor.workers))
            self.assertEqual(motor.activas, {})
            contenido = json.loads(next(Path(tmp).glob("estado_*.json")).read_text(encoding="utf-8"))
            self.assertEqual(len(contenido["tareas"]), 2)
            with self.assertRaises(RuntimeError):
                motor.encolar(Esperar(tipo="esperar", segundos=0))
            nuevo = Motor(datos=tmp)
            with self.assertLogs("motor", level="WARNING"):
                await nuevo.iniciar()
            await nuevo.cerrar()


if __name__ == "__main__":
    unittest.main()
