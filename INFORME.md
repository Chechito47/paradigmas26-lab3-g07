# INFORME — Laboratorio 3: Procesamiento distribuido con Apache Spark

**Materia:** Paradigmas de Programación 2026 — FAMAF  
**Laboratorio:** Lab 3 — Reddit NER con Apache Spark

---

## Ejercicio 1 — Identificar las regiones paralelizables

### a) Diagrama de flujo del pipeline

```
[Driver]  Leer suscripciones
          FileIO.readSubscriptions(filePath)
          │
          │  List[Subscription]
          ▼
[Driver]  sc.parallelize(subscriptions)
          │
          │  RDD[Subscription]
          ▼
[Workers] flatMap: descargar feed + parsear posts
          FileIO.downloadFeed(url) + JsonParser.parsePosts(json, name)
          │
          │  RDD[Post]
          ▼
[Workers] filter: descartar posts vacíos
          Analyzer.filterEmptyPosts equivalente: post.title.nonEmpty && post.selftext.nonEmpty
          │
          │  RDD[Post]
          ▼
[Workers] flatMap: extraer entidades nombradas     <-- List[NamedEntity] (del diccionario, cargado en driver)
          Analyzer.detectEntities(text, dictionary)
          │
          │  RDD[NamedEntity]
          ▼
[Workers] map: convertir a par clave-valor
          entity => ((entity.entityType, entity.text), 1)
          │
          │  RDD[((String, String), Int)]
          ▼
[Workers] reduceByKey: sumar conteos     <-- BARRERA DE SINCRONIZACIÓN
          │
          │  RDD[((String, String), Int)]
          ▼
[Driver]  collect() + sortBy(-count, type, name) + take(topK)
          │
          │  Array[((String, String), Int)]
          ▼
[Driver]  Formatear e imprimir
          Formatters.formatEntityStats(entityCounts, topK)
```

Para tener en cuenta: `Dictionary.loadAll(entitiesDir)` devuelve un `List[NamedEntity]` y se ejecuta en el **driver** antes de que comience el pipeline distribuido. Ese valor se serializa y se envía a cada worker cuando se ejecuta el `flatMap` de extracción de entidades.

---

### b) Clasificación de cada paso según la abstracción de Spark

| Paso | Abstracción Spark | Justificación |
|------|-------------------|---------------|
| Leer suscripciones desde JSON | *(ninguna — se ejecuta en el driver)* | Operación local sobre un archivo. El resultado se pasa luego a `sc.parallelize`. |
| `sc.parallelize(subscriptions)` | *(acción de creación de RDD)* | No es una transformación sino el punto de entrada al grafo distribuido. Distribuye los elementos del `List[Subscription]` entre los workers. |
| Descargar feed + parsear posts | `flatMap` | Cada `Subscription` produce cero o más `Post` (puede fallar la descarga → 0 posts, o devolver N posts). Es independiente por feed. |
| Filtrar posts vacíos | `filter` | Cada `Post` se evalúa de forma independiente. `filter` es un caso especial de `flatMap` (devuelve 0 o 1 elemento), pero Spark lo expone como abstracción propia. |
| Extraer entidades nombradas | `flatMap` | Cada `Post` produce cero o más `NamedEntity`. La cantidad varía según el contenido del post. Es independiente entre posts. |
| Convertir entidad a par clave-valor | `map` | Cada `NamedEntity` produce exactamente un par `((String, String), Int)`. Relación 1-a-1, sin dependencia entre elementos. |
| Sumar conteos por entidad | `reduceByKey` | El conteo total de una entidad depende de todos los posts de todos los workers. Es imposible calcularlo sin combinar resultados globalmente. |
| Ordenar y tomar top-K | `sortBy` + `take` (acción) | `sortBy` es una transformación que produce un RDD ordenado. `take` es una acción que materializa el resultado en el driver. |
| Formatear e imprimir | *(ninguna — se ejecuta en el driver)* | Operación local sobre los datos ya recolectados con `collect`/`take`. |

**¿Hay algún paso que no encaje en ninguna de estas abstracciones?**

Sí, dos pasos no son transformaciones distribuidas:

1. **Leer el archivo de suscripciones y cargar el diccionario** (`FileIO.readSubscriptions`, `Dictionary.loadAll`): son operaciones de I/O local que ocurren completamente en el driver, antes de que Spark entre en escena. No tienen sentido como operaciones distribuidas porque dependen de un único archivo accesible desde el driver.

2. **Formatear e imprimir el resultado final**: ocurre en el driver luego de `collect`. Es lógica de presentación sobre datos ya agregados; distribuirla no tendría beneficio.

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

## Decisiones de diseño relevantes (opcional no se si hace falta)

TO DO: cuando esten ejercicios 2 a 5