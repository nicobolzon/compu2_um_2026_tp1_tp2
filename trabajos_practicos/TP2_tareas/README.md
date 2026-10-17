# TP2 - Plataforma de tareas

Implementacion de la [consigna oficial](consigna.md) de Computacion II.
Entrega: 3 de noviembre de 2026. El informe incluye mediciones reales,
decisiones de diseno, limitaciones y reflexion personal.

## Ejecutar

Con Docker Desktop iniciado, desde `trabajos_practicos/TP2_tareas`:

```powershell
docker compose up --build
```

Desde la raiz del repositorio, primero ejecutar
`cd trabajos_practicos/TP2_tareas`: el Compose de la raiz corresponde al TP1.
La API queda en http://localhost:8000 y su documentacion interactiva en
http://localhost:8000/docs. Cerrar con Ctrl+C para que el contenedor reciba
SIGTERM y guarde el estado. Los volcados permanecen en
`datos/estado_<timestamp>.json` mediante un volumen. Al iniciar se informa el
ultimo volcado encontrado, pero las tareas no se restauran automaticamente.

Tambien puede ejecutarse sin Docker con Python 3.11 o superior. En PowerShell,
desde `trabajos_practicos/TP2_tareas`:

```powershell
py -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m uvicorn app:app --host 127.0.0.1 --port 8000
```

No iniciar ambas variantes a la vez: usan el mismo puerto 8000. Usar **un solo
proceso Uvicorn**, porque la cola y el registro estan en memoria y no se
comparten entre procesos.

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
$tarea = Invoke-RestMethod http://127.0.0.1:8000/tareas -Method Post -ContentType application/json -Body '{"tipo":"esperar","segundos":2,"timeout":10}'
$tarea.id
Invoke-RestMethod "http://127.0.0.1:8000/tareas/$($tarea.id)"
Invoke-RestMethod http://127.0.0.1:8000/estadisticas
```

La primera consulta puede mostrar `pendiente` o `ejecutando`; repetirla despues
de dos segundos para ver `completada`. Para probar la cancelacion, crear una
tarea `esperar` de 30 segundos y enviar `DELETE /tareas/{id}` antes de que termine.

Ejemplo curl (Bash; reemplazar `ID` por el `id` devuelto por POST):

```bash
curl -X POST http://localhost:8000/tareas -H 'Content-Type: application/json' -d '{"tipo":"esperar","segundos":30,"timeout":60}'
curl http://localhost:8000/tareas/ID
curl -X DELETE http://localhost:8000/tareas/ID
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
  La medicion de CPU incluida abajo muestra menor latencia de la API, no un
  hash mas rapido. Se eligen hilos por la cancelacion cooperativa sencilla;
  un executor de procesos aislaria el GIL, pero requeriria otro mecanismo IPC
  para cancelar el calculo en curso.
- Cancelar un Future no detiene su hilo. Una Event solicita al hash salir
  cada 1024 iteraciones y la corutina espera esa salida antes de liberar el worker.
- Cada worker captura fallos de tareas y continua con la siguiente.
  TaskGroup cancela descargas hermanas ante un error; los contextos de streaming
  cierran las respuestas. Un HTTP 4xx/5xx se registra como respuesta;
  un fallo de transporte hace fallar la tarea.
  Una excepcion que escapara del bucle del worker mataria esa corutina y
  reduciria la capacidad del pool. No hay un supervisor que la reinicie;
  por eso se capturan las excepciones de cada trabajo dentro del bucle.
- El timeout empieza cuando un worker toma la tarea, no mientras espera en cola.
- Uvicorn gestiona las senales y ejecuta el cierre lifespan. Se dejan de aceptar
  tareas, se cancelan las pendientes, se espera un plazo por las activas y
  luego se cancelan y liberan sus recursos. El JSON se escribe atomicamente.
- Clase 11: productor-consumidor. La cola desacopla HTTP y ejecucion;
  el registro se modifica en un solo event loop, sin await entre reservar
  el ID, encolar y publicar. No necesita un Lock de threads en ese tramo.
- Clases 17 y 18: multiplexacion y corutinas. Await cede el control durante
  I/O, pero no convierte un bucle de SHA-256 en trabajo no bloqueante.
- Clase 19: FastAPI, validacion del cuerpo y codigos HTTP (201/404/409/422/503).
- Clases 20 y 21: HTTP asincrono, TaskGroup, Queue, timeout y cancelacion.
- Clases 6 y 10: senales e hilos. Uvicorn gestiona las senales; el hilo de
  hashing recibe una Event de cancelacion, no una senal de terminacion forzada.

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

Resultado del 14/10/2026: **10 pruebas correctas**. Incluyen cierre del stream
al cancelar una descarga, liberacion del worker tras timeout de hashing,
y finalizacion normal de una tarea corta durante el plazo de apagado.
Tambien pasaron las 5 pruebas del TP1, sin modificar sus archivos.

`docker compose up -d --build` construyo la imagen y levanto la API el
14/10/2026 con Docker Engine 29.6.2. Desde el host, `/docs` respondio 200 y
las tareas `esperar` y `hashear` se completaron. Un `docker compose stop`
envio SIGTERM: con tareas en curso, el volcado guardo 7 registros (6
completados y 1 cancelado). Al reiniciar, el log informo el volcado previo.
Un `docker kill --signal=SIGINT` finalizo con codigo 0 y guardo otro JSON.
Ambos archivos quedaron en el volumen `datos/` del host.

## Mediciones obligatorias

Resultados completos: [mediciones_resultados.json](mediciones_resultados.json).
Banco de pruebas: [mediciones.py](mediciones.py). Reproduccion desde este directorio:

```powershell
.\.venv\Scripts\python.exe mediciones.py --repeticiones 3 --iteraciones 2000000
```

Medido el 14/10/2026 en Windows 11, Python 3.12.14, CPU AMD64 Family 25 Model 97,
32 procesadores logicos. FastAPI 0.142.0, Uvicorn 0.54.0, HTTPX 0.28.1 y
Pydantic 2.13.5. Los valores de las tablas son **medianas de 3 ejecuciones**;
el JSON conserva cada muestra. El cliente corre fuera del proceso del servidor.
Se usan puertos locales libres y las instancias se apagan al terminar.

### 1. Concurrencia I/O

Se sirven respuestas HTTP reales de 4096 bytes por loopback, con una espera
asincrona configurada de 100 ms. No se utiliza Internet ni MockTransport.
Secuencial: 20 tareas de una URL, esperando cada resultado antes de encolar
la siguiente. Concurrente: una tarea con las mismas 20 URLs. Se mide desde
el envio hasta observar el resultado completo, incluyendo HTTP y sondeo.

| Modalidad | Tiempo mediano |
| --- | ---: |
| 20 descargas secuenciales | 1.9224 s |
| 20 descargas concurrentes | 0.2191 s |
| Speedup (secuencial / concurrente, mediana) | 8.78x |

Las esperas se solapan. No se alcanza 20x: el semaforo permite 16 descargas
a la vez, por lo que se necesitan dos tandas, ademas del costo HTTP/sondeo.
La prueba compara el flujo completo de uso, no solamente sockets: la variante
secuencial tambien paga 20 altas de tarea. La demora de asyncio es nominal;
la resolucion del reloj y la planificacion en Windows afectan los tiempos
cortos (por eso no se deduce un limite exacto de 2 segundos).
Estos resultados no estiman el rendimiento de descargas grandes por Internet.

### 2. CPU y event loop

Se ejecutan 2.000.000 iteraciones SHA-256. El servidor avisa mediante una Event
de multiprocessing que empieza el calculo; entonces el cliente externo mide
un GET /tareas. La variante bloqueante existe solo en el proceso del banco,
no como opcion de la API publicada. Ambas variantes usan el mismo hash.

| Ejecucion del hash | Latencia GET mediana |
| --- | ---: |
| Directamente en la corutina (incorrecto) | 1041.98 ms |
| Con ThreadPoolExecutor | 39.15 ms |

El GET directo espera practicamente todo el hash. Con executor responde mientras
el calculo sigue activo: la mediana es unas 26.6 veces menor. No es latencia
cero: las muestras con executor fueron 36.63, 39.15 y 76.56 ms, y el GIL sigue
compartido. La duracion del hash ronda 1.05 s en ambas variantes. El beneficio
medido es capacidad de respuesta, no aceleracion de CPU ni paralelismo multinucleo.

### 3. Techo del pool

Pool de 4 workers, cola de 100. Se encolan lotes de tareas esperar de 0.2 s sin
esperar que termine la anterior. No son requests HTTP simultaneos: son trabajos
concurrentes pendientes/activos. Las latencias por tarea incluyen la cola,
desde creada hasta finalizada; el lote incluye el envio y el sondeo del cliente.

| Tareas | Lote (s) | Latencia media (s) | Latencia maxima (s) | Tareas/s |
| ---: | ---: | ---: | ---: | ---: |
| 4 | 0.2022 | 0.1831 | 0.1908 | 19.78 |
| 8 | 0.3881 | 0.2747 | 0.3740 | 20.61 |
| 16 | 0.7595 | 0.4592 | 0.7403 | 21.07 |
| 32 | 1.5078 | 0.8274 | 1.4756 | 21.22 |

Al superar cuatro tareas, las restantes esperan turnos. El throughput se
mantiene cerca de 20 tareas/s (4 / 0.2, con variacion de temporizadores), mientras
la latencia maxima crece por tandas. El limite observado es el pool, no la CPU.
No se generaliza a hashing o trafico externo. Para 10.000 envios de golpe,
la cola acotada rechaza con 503 cuando sus 100 lugares estan ocupados;
el registro de tareas terminadas sigue creciendo y es una limitacion separada.

## Limites actuales

Sin autenticacion; usar solo en un entorno de practica de confianza.
Las URLs pueden acceder a recursos internos del servidor: no publicar
abiertamente esta API. Maximo 100 URLs por tarea y 10 MiB descomprimidos por
respuesta. Maximo 10 millones de iteraciones de hash y timeout de 3600 s.
Los registros terminados permanecen en memoria hasta reiniciar. Una tarea
pendiente cancelada libera su lugar cuando el worker la retira de la cola.
El volcado no es persistencia transaccional y no sobrevive a un cierre forzado.

## Reflexion personal

La diferencia principal que puedo observar en este trabajo es que concurrencia
no significa ejecutar todo en paralelo. En las descargas se aprovechan los
tiempos de espera para atender otras operaciones. En cambio, escribir async
delante de una funcion de hashing no evita que su bucle ocupe el event loop.
La comparacion del GET muestra por que conviene separar ese calculo con un executor.

Otro punto importante es que cancelar una tarea no significa borrar su registro
ni detener automaticamente un hilo. El estado cancelada conserva informacion
para el cliente, las descargas deben cerrar sus respuestas y el hash necesita
una indicacion cooperativa de salida. Las pruebas de cancelacion permiten
comprobar esos comportamientos, ademas de verificar el camino exitoso.

Por ultimo, las mediciones muestran un limite concreto del diseno: aumentar las
tareas pendientes no aumenta la cantidad de workers. Con cuatro workers crece
la espera en cola cuando llegan mas trabajos, aunque la API siga respondiendo.
Por eso es importante medir latencia y capacidad, limitar la cola y documentar
que el estado en memoria y los volcados no equivalen a una base de datos durable.
