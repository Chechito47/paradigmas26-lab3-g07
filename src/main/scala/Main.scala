import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    // --- PASO 1: Parsear argumentos de linea de comandos (driver) ---
    val cmdArgs = CommandLineArgs.parse(args) match {
      case Some(parsed) => parsed
      case None => return // scopt prints error messages
    }


    // --- PASO 2: Leer suscripciones (driver) ---
    // Left = error fatal, Right = lista de suscripciones validas
    // (no hay que filtrar, ya vienen solo las suscripciones validas sin None)
    val subscriptions = FileIO.readSubscriptions(cmdArgs.subscriptionFile) match {
      // Error fatal: archivo no encontrado o JSON invalido
      case Left(errorMsg) =>
        println(errorMsg)
        return
      // Caso lista vacia
      case Right(subs) if subs.isEmpty =>
        println("Error: No valid subscriptions found")
        return
      // Devuelvo lista de suscripciones
      case Right(subs) =>
        subs
    }

    // Verificamos que el directorio de entidades existe antes de arrancar Spark
    val entitiesDir = new java.io.File(cmdArgs.entitiesDir)
    if (!entitiesDir.exists() || !entitiesDir.isDirectory) {
      println(s"Error: entities directory '${cmdArgs.entitiesDir}' not found")
      return
    }


    // --- PASO 3: Crear SparkSession (driver) ---
    val spark = SparkSession.builder()
      .appName("RedditNER")
      .master("local[*]")
      .getOrCreate()
    // Silenciamos los logs de Spark
    spark.sparkContext.setLogLevel("ERROR")
    //sc es lo que se usa para los RDD
    val sc = spark.sparkContext

    // sc.parallelize distribuye a los workers
    // El resultado es un RDD[Subscription]: cada worker va a procesar algunas suscripciones
    val subscriptionsRDD = sc.parallelize(subscriptions)


    // --- PASO 4: Descargar feeds y parsear posts (worker) ---
    // flatMap espera que la función devuelva un Iterable: si hay 0 posts devolvemos List(),
    // si hay N posts devolvemos List(post1, post2, ...)
    val postsRDD = subscriptionsRDD.flatMap { subscription =>
      // Intentamos descargar el feed
      FileIO.downloadFeed(subscription.url, subscription.name) match {
        // Si la descarga fallo imprimimos warning y devolvemos lista vacia
        case None =>
          println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
          List.empty[Post]
        // Si la descarga es exitosa parseamos los posts del JSON
        case Some(content) =>
          JsonParser.parsePosts(content, subscription.name)
      }
    }


    // --- PASO 5: Filtrar posts vacios (worker) ---
    // Notar que no podemos usar mas filterEmptyPosts xq ahora tenemos un RDD
    val filteredPostsRDD = postsRDD.filter { post =>
      post.title.nonEmpty &&
      post.selftext.nonEmpty &&
      post.selftext.trim.nonEmpty
    }


    // --- PASO INTERMEDIO: Calcular estadisticas (worker) ---
    // count() es una accion, entonces aca empieza realmente la ejecucion (lo otro era lazy)
    // Las estadisticas que faltan son parte del ejercicio 4
    val totalPosts    = postsRDD.count()
    val filteredCount = filteredPostsRDD.count()
    val removedCount  = totalPosts - filteredCount

    // Calculamos el promedio de caracteres sobre los posts filtrados
    val avgChars = if (filteredCount > 0) {
      filteredPostsRDD
        .map(post => post.title.length + post.selftext.length)  // RDD[Int]
        .reduce(_ + _) / filteredCount                          // suma total / cantidad
    } else 0L

    // Imprimimos las estadísticas de descarga
    // feedsSuccess/feedsFailed son para el Ejercicio 4
    // Por ahora solo pongo lo que tenemos
    println(s"============ ESTADISTICAS DE PROCESAMIENTO ============")
    println(s"Posts descargados: $totalPosts")
    println(s"Posts filtrados (vacíos/nulos): $removedCount")
    println(s"Posts válidos: $filteredCount")
    println(s"Largo promedio en posts: $avgChars")
    println()

    // Verificamos si no hay mas posts que procesar
    if (filteredCount == 0) {
      println("Error: No valid posts downloaded after filtering")
      spark.stop()
      return
    }


    // --- PASO 6: Cargar diccionario (driver) ---
    // El diccionario se carga una sola vez en el driver
    // Spark lo serializa y envia a cada worker
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)


    // --- PASO 7: Extraer entidades nombradas (worker) ---
    // Para cada post, detectamos todas las entidades que aparecen en su texto
    val entitiesRDD = filteredPostsRDD.flatMap { post =>
      val combinedText = post.title + " " + post.selftext
      Analyzer.detectEntities(combinedText, dictionary)
    }


    // --- PASO 8: Convertir a par clave valor (worker) ---
    // La clave es (tipo, nombre) para contar por entidad unica
    val paresRDD = entitiesRDD.map { entity =>
      ((entity.entityType, entity.text), 1)
    }


    // --- PASO 9: Conteo de entidades (worker) ---
    // Resultado: RDD[((String, String), Int)] con el conteo por entidad
    val conteosRDD = paresRDD.reduceByKey(_ + _)


    // --- PASO 10: Recolectar (driver) ---
    val resultados = conteosRDD.collect()


    // --- PASO 11: Formatear e imprimir (driver) ---
    // Ordenamos: primero por conteo descendente, luego por tipo, luego por nombre
    val ordenados = resultados
      .sortBy { case ((tipo, nombre), conteo) => (-conteo, tipo, nombre) }
      .take(cmdArgs.topK)

    // Imprimimos el ranking de entidades
    println(s"============ ENTIDADES NOMBRADAS MAS FRECUENTES ============")
    ordenados.foreach { case ((tipo, nombre), conteo) =>
      println(s"[Type=$tipo] $nombre: $conteo apariciones")
    }

    // Cerramos la SparkSession al terminar
    spark.stop()
  }
}
