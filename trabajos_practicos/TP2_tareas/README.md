# TP2 - Plataforma de tareas

Implementacion inicial de la [consigna oficial](consigna.md).
Entrega: 3 de noviembre de 2026. **No es todavia la entrega final**:
faltan las mediciones obligatorias, su analisis y la reflexion personal.

## Ejecutar

Python 3.11 o superior. Desde este directorio, en PowerShell:

```powershell
py -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m uvicorn app:app --host 127.0.0.1 --port 8000
```

Documentacion interactiva: http://127.0.0.1:8000/docs.
Cerrar con Ctrl+C para guardar el estado. Usar **un solo proceso Uvicorn**:
la cola y el registro estan en memoria, no se comparten entre procesos.

Con Docker Desktop iniciado, desde este directorio:

```powershell
docker compose up --build
```

API en http://localhost:8000/docs. Los volcados se conservan en
`datos/estado_<timestamp>.json` mediante un volumen. No se restauran tareas
automaticamente: al iniciar se informa el ultimo volcado encontrado.

## API

- POST /tareas: valida y encola, devuelve 201 con ID y estado pendiente.
- GET /tareas: lista; filtros opcionales estado y limite (1 a 1000).
- GET /tareas/{id}: estado, resultado, error y tiempos; 404 si no existe.
- DELETE /tareas/{id}: cancela en cola o ejecutando, conserva el registro;
  409 si ya termino y 404 si no existe.
- GET /estadisticas: totales por estado, duracion media de tareas que
  llegaron a ejecutarse y terminaron, workers ocupados y capacidad del pool.

Cuerpos JSON para POST, tambien disponibles en Swagger:

```json
{"tipo":"esperar","segundos":2,"timeout":10}
```
```json
{"tipo":"hashear","texto":"hola","iteraciones":100000,"timeout":30}
```
```json
{"tipo":"descargar","urls":["https://example.com","https://www.python.org"],"timeout":30}
```

Ejemplo PowerShell:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/tareas -Method Post -ContentType application/json -Body '{"tipo":"esperar","segundos":2,"timeout":10}'
Invoke-RestMethod http://127.0.0.1:8000/tareas/1
Invoke-RestMethod http://127.0.0.1:8000/estadisticas
```

Ejemplo curl (Bash):

```bash
curl -X POST http://localhost:8000/tareas -H 'Content-Type: application/json' -d '{"tipo":"esperar","segundos":2,"timeout":10}'
curl http://localhost:8000/tareas/1
curl -X DELETE http://localhost:8000/tareas/1
```

## Arquitectura y decisiones

```text
HTTP -> FastAPI -> registro en memoria + asyncio.Queue acotada
                                      |
                                N corutinas worker
                                      |
                       +--------------+-------------+
                       |              |             |
                  httpx async    ThreadPool     asyncio.sleep
                  descargar      hashear        esperar
                                      |
                          resultado / error / estado
Apagado lifespan -> espera acotada -> cancela -> cierra -> JSON
```

- asyncio.Queue permite esperar sin bloquear el event loop; queue.Queue
  esta pensada para hilos y su get bloqueante detendria la API.
- El pool usa corutinas. Las descargas usan un cliente HTTP compartido y
  TaskGroup, con hasta 16 descargas simultaneas entre todas las tareas.
- El hash parte del texto UTF-8 y aplica SHA-256 a los bytes del digest
  anterior en cada iteracion; devuelve el ultimo digest hexadecimal.
  Se ejecuta en ThreadPoolExecutor para sacar el bucle CPU de la corutina.
  Esto no promete paralelismo multinucleo: el GIL puede degradar la latencia.
  La medicion obligatoria debera cuantificarlo antes de cerrar esta decision.
- Cancelar un Future no detiene su hilo. Una Event solicita al hash salir
  cada 1024 iteraciones y la corutina espera esa salida antes de liberar el worker.
- Cada worker captura fallos de tareas y continua con la siguiente.
  TaskGroup cancela descargas hermanas ante un error; los contextos de streaming
  cierran las respuestas. Un HTTP 4xx/5xx se registra como respuesta;
  un fallo de transporte hace fallar la tarea.
- El timeout empieza cuando un worker toma la tarea, no mientras espera en cola.
- Uvicorn gestiona las senales y ejecuta el cierre lifespan. Se dejan de aceptar
  tareas, se cancelan las pendientes, se espera un plazo por las activas y
  luego se cancelan y liberan sus recursos. El JSON se escribe atomicamente.
- Relacion con la materia: multiplexacion de I/O, corutinas, sincronizacion,
  colas productor-consumidor, hilos, cancelacion y manejo de senales.

Configuracion por variables de entorno:

| Variable | Por defecto | Uso |
| --- | --- | --- |
| TP2_WORKERS | 4 | Corutinas worker y maximo de hilos del executor |
| TP2_COLA | 100 | Tareas esperando; cola llena devuelve 503 |
| TP2_APAGADO | 5 | Segundos de gracia para tareas activas |
| TP2_DATOS | datos | Directorio de volcados |

## Pruebas

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Pruebas locales sin Internet: validacion, estados, resultados de hash,
cancelacion activa/pendiente, saturacion, timeout, recuperacion tras fallos,
descargas concurrentes simuladas y cierre con volcado y deteccion al reiniciar.

## Limites actuales

Sin autenticacion; usar solo en un entorno de practica de confianza.
Las URLs pueden acceder a recursos internos del servidor: no publicar
abiertamente esta API. Maximo 100 URLs por tarea y 10 MiB descomprimidos por
respuesta. Maximo 10 millones de iteraciones de hash y timeout de 3600 s.
Los registros terminados permanecen en memoria hasta reiniciar. Una tarea
pendiente cancelada libera su lugar cuando el worker la retira de la cola.
El volcado no es persistencia transaccional y no sobrevive a un cierre forzado.

## Pendiente para la entrega

- Medir 20 descargas secuenciales y concurrentes, calcular speedup.
- Comparar latencia GET durante CPU directa y CPU en executor.
- Medir saturacion al aumentar la concurrencia y analizar degradacion.
- Registrar equipo, versiones, comandos y resultados reproducibles.
- Verificar Docker y senales con pruebas de integracion en el entorno de entrega.
- Escribir 2-3 parrafos personales de aprendizaje y revisar el informe final.

No se incluyen cifras inventadas ni una reflexion personal prefabricada.
