# INFORME — Laboratorio 3: Procesamiento distribuido con Apache Spark

**Materia:** Paradigmas de Programación 2026 — FAMAF  
**Laboratorio:** Lab 3 — Reddit NER con Apache Spark
**Grupo:** Nº 7
**Integrantes**:
* Aranda, María Jose
* Pinilla, Lautaro
* Sabino, Lucas
* Sosa, Santiago

---

## Índice
1. [Ejercicio 1](#ejercicio-1--identificar-las-regiones-paralelizables)
2. [Ejercicio 2](#2-introducción-problema-a-estudiar)
3. [Ejercicio 3](#3-métodos-modelo-y-parámetros)

---

## Ejercicio 1 — Identificar las regiones paralelizables

### a) Diagrama de flujo del pipeline
Estos son los pasos del pipeline, indicando el tipo de entrada y salida, y si se ejecuta en el **driver** o en los **workers**:
```
1.  [Driver]  Lectura del archivo de suscripciones
              FileIO.readSubscriptions(filePath)
              │
              │  List[Option[Subscription]]
              ▼
2.  [Driver]  Filtrado de suscripciones malformadas (eliminar None)
              subscriptionOpts.flatten
              │
              │  List[Subscription]
              ▼
3.  [Driver]  Distribucion a los workers
              sc.parallelize(subscriptions)
              │
              │  RDD[Subscription]
              ▼
4.  [Workers] flatMap: descargar feed + parsear posts
              FileIO.downloadFeed(url) + JsonParser.parsePosts(json, name)
              │
              │  RDD[Post] (sin filtrar)
              ▼
5.  [Workers] filter: descartar posts vacíos
              Analyzer.filterEmptyPosts equivalente: post.title.nonEmpty && post.selftext.nonEmpty
              │
              │  RDD[Post] (posts con titulo y texto no vacos)
              │
6.  [Driver]  Carga del diccionario de entidades <-- String(entitiesDir)
              Dictionary.loadAll(entitiesDir)
              │
              │  List[NamedEntity]
              ▼
7.  [Workers] flatMap: extraer entidades nombradas
              Analyzer.detectEntities(text, dictionary)
              │
              │  RDD[NamedEntity]
              ▼
8.  [Workers] map: convertir a par clave-valor
              entity => ((entity.entityType, entity.text), 1)
              │
              │  RDD[((String, String), Int)]
              ▼
9.  [Workers] reduceByKey: conteo     <-- BARRERA DE SINCRONIZACIÓN
              Analyzer.countEntities(allEntities) && Analyzer.countByType(allEntities)
              │
              │  RDD[((String, String), Int)]
              ▼
10.  [Driver] Recolección al driver
              collect() + sortBy(-count, type, name) + take(topK)
              │
              │  Array[((String, String), Int)]
              ▼
11.  [Driver] Formatear e imprimir
              Formatters.formatEntityStats(entityCounts, topK)
```

Para tener en cuenta: `Dictionary.loadAll(entitiesDir)` devuelve un `List[NamedEntity]` y se ejecuta en el **driver** antes de que comience el pipeline distribuido. Ese valor se serializa y se envía a cada worker cuando se ejecuta el `flatMap` de extracción de entidades.

---

### b) Clasificación de cada paso según la abstracción de Spark

| Paso | ¿Transformación sobre RDD? | Abstracción Spark | Justificación |
|------|----------------------------|-------------------|----------------|
| 1 | Operación local | **Ninguna** | Operación local sobre un archivo. El resultado se pasa luego a `sc.parallelize` |
| 2 | Operación local | **Ninguna** | `flatten` local |
| 3 | Creación del RDD | **Ninguna** | `parallelize` no es transformación, sino el punto de entrada al grafo distribuido. Distribuye los elementos del `List[Subscription]` entre los workers |
| 4 | Sí (0..N posts) | `flatMap` | Cada `Subscription` produce cero o más `Post` (puede fallar la descarga → 0 posts, o devolver N posts) |
| 5 | Sí (0 o 1 post) | **Ninguna** | Es `filter`, no está en la lista (aunque es un caso especial de `flatMap`) |
| 6 | Operación local | **Ninguna** | Carga de diccionario en driver |
| 7 | Sí (0..N entidades) | `flatMap` | Cada `Post` produce cero o más `NamedEntity` |
| 8 | Sí (1 a 1) | `map` | Cada `NamedEntity` produce exactamente un par `((String, String), Int)`. Relación 1-a-1, sin dependencia entre elementos |
| 9 | Sí (agrupa y suma) | `reduceByKey` | Conteo global por clave, depende de todos los posts de todos los workers |
| 10 | No es transformación (acción) | **Ninguna** | `collect` es una acción, no transformación |
| 11 | Operación local | **Ninguna** | Ordenamiento e impresión en driver |

**¿Hay algún paso que no encaje en ninguna de estas abstracciones?**  
Si, los pasos 1, 2, 3, 5, 6, 10 y 11:
* Los pasos 1, 2, 3 y 6 por ser operaciones locales del driver
* El paso 5 porque es `filter` (Spark lo tiene, pero no es `map`/`flatMap`/`reduceByKey`)
* El paso 10 porque es una acción
* El paso 11 porque es ordenamiento local

---

### c) Barreras de sincronización vs. pasos independientes

**Pasos que son barreras de sincronización:**

- **`reduceByKey`**: es la única barrera del pipeline. Para calcular el conteo total de, por ejemplo, la entidad `("ProgrammingLanguage", "Scala")`, Spark necesita combinar los conteos parciales de *todos* los workers. Ningún worker puede producir el resultado final hasta que todos hayan terminado de procesar sus particiones y hayan enviado sus resultados parciales al shuffle. El resultado del `reduceByKey` no está disponible hasta que la etapa completa termina.

- **`collect()` / `take()`**: técnicamente no son barreras de transformación, pero sí son **acciones terminales** que fuerzan la ejecución completa del grafo (DAG) hasta ese punto. Todo el pipeline lazy se materializa en este momento.

**Pasos que pueden ejecutarse de forma completamente independiente entre workers:**

- `flatMap` de descarga de feeds: cada worker descarga su propio subconjunto de URLs sin coordinarse con otros. Un worker que descarga `r/scala` no necesita esperar al que descarga `r/programming`.

- `filter` de posts vacíos: la decisión de descartar un post depende únicamente de ese post, no de los demás.

- `flatMap` de extracción de entidades: cada worker procesa sus posts de forma local. La detección de `"Scala"` en un post no depende de ningún otro post.

- `map` de clasificación: transformación 1-a-1, completamente independiente entre elementos.

---

### d) Restricciones que impone Spark sobre las funciones del desarrollador

Las funciones que se pasan a las transformaciones de Spark (`map`, `flatMap`, `reduceByKey`, etc.) deben cumplir tres restricciones fundamentales para funcionar correctamente en un entorno distribuido:

**1. Serializable**

La función (closure) se serializa con Java serialization y se envía por red a cada worker. Cualquier objeto capturado en el closure debe ser `Serializable`. Si la función captura una referencia a un objeto no serializable (por ejemplo, una conexión a base de datos, un `SparkContext`, o una instancia de una clase que no implementa `Serializable`), Spark lanzará una `NotSerializableException` en tiempo de ejecución.

En este proyecto: el `Dictionary` cargado en el driver se captura en el closure del `flatMap` de extracción de entidades. La `List[NamedEntity]` es serializable (son case classes / clases simples), por lo que no hay problema. Sin embargo, si se capturara el objeto `Dictionary` como un singleton con estado, podría ser problemático.

**2. Sin estado compartido mutable**

Spark ejecuta las funciones en múltiples JVMs distintas (los workers). No existe memoria compartida entre ellas. Cualquier variable mutable definida fuera del closure y usada dentro de él **no se comparte**: cada worker tiene su propia copia serializada. Intentar usar una variable compartida (como un contador `var total = 0` modificado dentro de un `map`) producirá resultados incorrectos silenciosamente, porque las modificaciones ocurren en copias locales que nunca se propagan al driver.

La solución correcta para contadores es usar **Accumulators** (Ejercicio 4), que son el único mecanismo seguro de comunicación worker → driver.

**3. Sin efectos secundarios observables (idealmente)**

Las funciones deben ser lo más puras posible. Spark puede re-ejecutar tareas en caso de fallo (tolerancia a fallos), lo que significa que una función puede ejecutarse más de una vez sobre los mismos datos. Si la función tiene efectos secundarios como escribir a disco, enviar requests HTTP o modificar una base de datos, esas operaciones se repetirían, causando duplicados o inconsistencias.

En este proyecto: `FileIO.downloadFeed()` dentro del `flatMap` es un efecto secundario (request HTTP). Esto es aceptable y esperado para este caso de uso, pero hay que ser consciente de que ante un fallo y re-ejecución, el feed se descargaría dos veces.

**Resumen:**

| Restricción | Consecuencia si se viola |
|-------------|--------------------------|
| No serializable | `NotSerializableException` en runtime |
| Estado compartido mutable | Resultados incorrectos silenciosos |
| Efectos secundarios | Operaciones duplicadas ante re-ejecución |

---

# Decisiones de diseño relevantes (opcional no se si hace falta)
## Ejercicio 2 — Paralelizar la descarga de feeds
### readSubscriptions
Se nos pide manejar estos casos de error:
* Si el archivo no existe -> `Error: Could not load $filePath - file not found`
* Si el JSON es invalido -> `Error: Could not load $filePath - invalid JSON format`
* Si una suscripcion le falta name o url -> `Warning: Skipping malformed subscription (missing 'name' or 'url' field)`

*Los dos primeros son errores fatales => el programa termina*  
*La ultima es warning => se interrumpe la ejecucion sin terminar*

### downloadFeed
* Tenemos que agregar el argumento `subscriptionName: String` para poder hacer un warning relacionado a esta funcion

### main
* Si no hay suscripciones válidas -> `Error: No valid subscriptions found`
* Si no se puede hacer fetch de una url -> `"Warning: Failed to download from '${subscription.name}' (${subscription.url})" (aca es donde sucede)`
* No llamamos a filterEmptyPosts ya que ahora tenemos un RDD y no se puede (deberiamos cambiar la implementacion para eso)
* Tenemos solo 3 estadisticas: **totalPosts, filteredCount, removedCount**. Las que faltan son parte del ejercicio 4
* Se adapto el formateo

### CommandLineArgs
* Indicamos que lea el archivo del mock `local_subcriptions.json`

### NamedEntity
* Hicimos que la clase `NamedEntity` extienda a `Serializable`, para que funcione con Spark

**Nota**: Los errores que aparecen al ejecutar son normales de spark