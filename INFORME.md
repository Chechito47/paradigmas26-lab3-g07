# Informe — Laboratorio 3: Procesamiento distribuido con Apache Spark

---

## Ejercicio 1 — Identificar las regiones paralelizables

### a) Diagrama de flujo del pipeline

El pipeline se puede representar como una secuencia casi lineal. Cada nodo indica quién ejecuta ese paso (driver o worker) y cada arista indica el tipo Scala del dato que fluye entre pasos.

```
[Driver] Leer suscripciones
         FileIO.readSubscriptions(filePath)
         │
         │  List[Subscription]
         ▼
[Driver] sc.parallelize(subscriptions)
         │
         │  RDD[Subscription]
         ▼
[Worker] flatMap: descargar feed + parsear posts
         downloadFeed(url) → parsePosts(content)
         Cada worker recibe un subconjunto de suscripciones,
         descarga su feed y emite 0..N posts por suscripción.
         ← .cache() aplicado aquí →
         │
         │  RDD[Post]
         ▼
[Worker] filter: descartar posts vacíos
         title y selftext no vacíos ni blancos
         ← .cache() aplicado aquí →
         │
         │  RDD[Post]
         ▼
[Acción/Barrera] count() sobre postsRDD y filteredPostsRDD
         + lectura de acumuladores (feedsSuccess, feedsFailed,
           postsSuccess, postsFiltered)
         │
         │  Long (escalares al driver)
         ▼
[Worker] map + reduce: calcular promedio de caracteres
         post → (title.length + selftext.length), luego reduce(_ + _)
         │
         │  Long (escalar al driver)
         ▼
[Driver] Cargar diccionario
         Dictionary.loadAll(entitiesDir)
         El resultado es serializado por Spark y enviado a cada worker.
         │
         │  List[NamedEntity]  (broadcast implícito en el cierre del flatMap)
         ▼
[Worker] flatMap: extraer entidades nombradas
         Analyzer.detectEntities(text, dictionary)
         Cada post puede generar 0..N entidades.
         │
         │  RDD[NamedEntity]
         ▼
[Worker] map: entidad → par clave-valor
         entity → ((entity.entityType, entity.text), 1)
         │
         │  RDD[((String, String), Int)]
         ▼
[Worker + Barrera] reduceByKey(_ + _)
         Agrega conteos por clave (tipo, nombre).
         Barrera de sincronización: ningún worker puede producir
         el resultado parcial hasta que todos hayan emitido sus pares.
         │
         │  RDD[((String, String), Int)]
         ▼
[Driver] collect()
         Trae todos los resultados al driver.
         │
         │  Array[((String, String), Int)]
         ▼
[Driver] sortBy + take(topK) + Formatters.formatEntityStats
         Ordena, recorta al top-K e imprime.
```

---

### b) Correspondencia con abstracciones de Spark

Analisis de cada paso del pipeline y que abstraccion de Spark le corresponde

**`flatMap` — descarga y parseo de feeds**

Cada suscripción es independiente, no necesita información de ninguna otra para descargar su feed. La cantidad de posts que genera es variable (0 si la descarga falla, N si tiene éxito), por lo que el uso de `flatMap` encaja. No se podría usar `map` porque exige exactamente una salida por entrada, y una suscripción puede producir cero posts (error de red, feed vacío) o muchos.

**`filter` — descartar posts vacíos**

Cada post se evalúa de forma independiente contra un predicado. `filter` es un caso especial de `flatMap` donde la salida es 0 o 1 elementos (el mismo elemento si pasa, nada si no). En el código se usa `filter` directamente, que es más expresivo.

**`map` — calcular longitud de caracteres por post**

Transforma cada `Post` en un `Int` (longitud de título + selftext). Exactamente una salida por entrada, sin dependencia entre posts. Es el caso canónico de `map`.

**`reduce` — sumar longitudes para calcular el promedio**

Combina todos los `Int` del RDD en un único escalar usando el operador suma. No es `reduceByKey` porque no hay agrupamiento por clave. Es una reducción total sobre todo el RDD. Es una acción terminal (trae el resultado al driver), no una transformación.

**`flatMap` — extracción de entidades nombradas**

Cada post puede producir cero o más `NamedEntity` dependiendo de cuántas entidades del diccionario aparezcan en su texto. Nuevamente, la variabilidad en la cantidad de salidas hace que `flatMap` sea la abstracción correcta.

**`map` — entidad a par clave-valor**

Convierte cada `NamedEntity` en `((entityType, text), 1)`. Exactamente una salida por entrada

**`reduceByKey` — contar ocurrencias por entidad**

Agrupa todos los pares por clave `(tipo, nombre)` y suma los valores. El resultado de una clave depende de todos los elementos con esa clave distribuidos entre todos los workers, por lo que es imposible computarlo sin una barrera de sincronización. Es el caso canónico de `reduceByKey`.

**Pasos que no encajan en ninguna de las tres abstracciones**

Hay dos pasos que no son transformaciones de Spark:

1. **Leer suscripciones del archivo JSON** (`FileIO.readSubscriptions`): es una operación de I/O local que corre íntegramente en el driver. Su salida es una `List[Subscription]` que luego se paraleliza con `sc.parallelize`. No tiene sentido distribuirla porque el archivo es local y la lista es pequeña.

2. **Cargar el diccionario de entidades** (`Dictionary.loadAll`): ídem, es I/O local en el driver. El resultado es una `List[NamedEntity]` que Spark serializa automáticamente y envía a cada worker como parte del cierre del `flatMap` de extracción de entidades.

3. **`collect` + ordenar + imprimir**: `collect` es una acción que trae datos de los workers al driver. El ordenamiento y la impresión posteriores son operaciones locales sobre un `Array`, fuera del modelo RDD. No hay una abstracción de Spark que corresponda a "traer todo al driver y procesarlo localmente", más allá de la propia acción `collect`.

---

### c) Barreras de sincronización

Una barrera es un punto donde todos los workers deben haber terminado su trabajo parcial antes de que el pipeline pueda continuar.

**Pasos que son barreras:**

- **`count()` sobre `postsRDD` y `filteredPostsRDD`**: son acciones terminales. Spark no puede devolver el conteo total hasta que todos los workers hayan procesado sus particiones. Hasta este punto la ejecución es completamente lazy; las barreras son lo que fuerza la ejecución real.

- **`reduce(_ + _)` para el promedio de caracteres**: reduce global que espera a todos los workers.

- **`reduceByKey(_ + _)`**: barrera de shuffle. Spark redistribuye todos los pares entre workers según la clave antes de reducir, lo que requiere que todos los workers hayan emitido sus pares.

- **`collect()`**: trae todos los datos de los workers al driver. El driver no puede continuar hasta que todos hayan terminado.

**Pasos que se ejecutan de forma completamente independiente entre workers:**

- El `flatMap` de descarga y parseo: cada worker descarga sus feeds sin coordinarse con los demás.
- El `filter` de posts vacíos: cada worker evalúa sus posts localmente.
- El `map` de longitud de caracteres: local por partición.
- El `flatMap` de extracción de entidades: cada worker procesa sus posts de forma independiente.
- El `map` de pares clave-valor: local por elemento.

La fase de `reduceByKey` tiene una parte de "map-side combine" que también puede ocurrir localmente en cada worker (reducción parcial antes del shuffle), pero el resultado final sí requiere sincronización.

---

### d) Restricciones sobre las funciones pasadas a las transformaciones de Spark

Para que una función pueda ejecutarse en un entorno distribuido, Spark impone las siguientes restricciones:

**Serialización**: la función se serializa (junto con todo lo que captura en su cierre) y se envía por red a cada worker. Todo objeto capturado por la función debe ser serializable con Java Serialization. En el proyecto esto explica por qué `NamedEntity` extiende `Serializable`: el diccionario completo de entidades es capturado por el cierre del `flatMap` de extracción, y Spark necesita enviarlo a cada worker. Si un objeto no es serializable, Spark lanza una excepción en tiempo de ejecución al intentar enviar la tarea.

**Estado compartido mutable**: las funciones no deben leer ni escribir estado mutable compartido (variables globales, singletons con estado). En un entorno distribuido, cada worker tiene su propia copia del proceso JVM; no existe memoria compartida entre workers. Si una función modifica un objeto "compartido", cada worker modifica su copia local y los cambios nunca se propagan. La única forma correcta de comunicar información de workers al driver es mediante `Accumulator`s, que Spark maneja de forma segura.

**Efectos secundarios**: en principio, las funciones deberían ser puras (sin efectos secundarios) porque Spark puede re-ejecutar tareas que fallen, lo que haría que los efectos secundarios se repitan. En el proyecto hay efectos secundarios intencionales: los `println` de warnings y los incrementos de acumuladores. Los `println` dentro del `flatMap` son aceptables en modo local (los logs aparecen en la misma consola), pero en un cluster real los mensajes se pierden en los logs de los workers y no se ven en el driver. En cuanto a los acumuladores, Spark garantiza que se incrementan exactamente una vez por tarea si la tarea tiene éxito, pero puede haber doble conteo si Spark re-ejecuta una tarea fallida antes de que la primera intente completarse.

**Conmutatividad y asociatividad para reducciones**: la función pasada a `reduceByKey` debe ser conmutativa (`f(a,b) == f(b,a)`) y asociativa (`f(f(a,b),c) == f(a,f(b,c))`). Spark puede combinar los valores en cualquier orden y en cualquier worker, así que si la función no cumple estas propiedades el resultado será no determinista. La suma `_ + _` cumple ambas.