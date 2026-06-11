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

    //acumuladores
    val feedsSuccess = sc.longAccumulator("feedsSuccess")
    val feedsFailed = sc.longAccumulator("feedsFailed")
    val postsSuccess = sc.longAccumulator("postsSuccess")
    val postsFailed = sc.longAccumulator("postsFailed")
    val postsFiltered = sc.longAccumulator("postsFiltered")

    // --- PASO 4: Descargar feeds y parsear posts (worker) ---
    // flatMap espera que la función devuelva un Iterable: si hay 0 posts devolvemos List(),
    // si hay N posts devolvemos List(post1, post2, ...)
    val postsRDD = subscriptionsRDD.flatMap { subscription =>
      // Intentamos descargar el feed
      FileIO.downloadFeed(subscription.url, subscription.name) match {
        // Si la descarga fallo imprimimos warning y devolvemos lista vacia
        case None =>
          feedsFailed.add(1)
          println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
          List.empty[Post]
          
        // Si la descarga es exitosa parseamos los posts del JSON
        case Some(content) =>
          val posts = JsonParser.parsePosts(content, subscription.name)
          
          if (posts.isEmpty){
            postsFailed.add(1)
            println(s"Warning: Failed to parse posts from '${subscription.name}' (${subscription.url})")}

          else {
            feedsSuccess.add(1)
            postsSuccess.add(posts.length)
          }
          posts
      }
    }.cache()

    // --- PASO 5: Filtrar posts vacios (worker) ---
    // Notar que no podemos usar mas filterEmptyPosts xq ahora tenemos un RDD
    val filteredPostsRDD = postsRDD.filter { post =>
    val isValid =
      post.title.nonEmpty &&
      post.selftext.nonEmpty &&
      post.selftext.trim.nonEmpty

    if (!isValid){
      postsFiltered.add(1)
    }
    isValid
    }.cache()

    // --- PASO INTERMEDIO: Calcular estadisticas ---
    // count() es una accion, entonces aca empieza realmente la ejecucion (lo otro era lazy)

    val startTotalPosts = System.currentTimeMillis()
    val totalPosts    = postsRDD.count()
    val endTotalPosts = System.currentTimeMillis()
    val totalPostsSeconds = (endTotalPosts - startTotalPosts) / 1000.0

    val startFilteredPosts = System.currentTimeMillis() 
    val filteredCount = filteredPostsRDD.count()
    val endFilteredPosts = System.currentTimeMillis() 
    postsRDD.unpersist()
    val filteredStageSeconds = (endFilteredPosts - startFilteredPosts) / 1000.0

    //valores de los acumuladores
    val feedsSuccessValue  = feedsSuccess.value
    val feedsFailedValue   = feedsFailed.value
    val postsSuccessValue  = postsSuccess.value
    val postsFailedValue = postsFailed.value
    val postsFilteredValue = postsFiltered.value

    // Calculamos el promedio de caracteres sobre los posts filtrados
    val startAvgChars = System.currentTimeMillis()
    val avgChars = if (filteredCount > 0) {
      filteredPostsRDD
        .map(post => post.title.length + post.selftext.length)  // RDD[Int]
        .reduce(_ + _) / filteredCount                          // suma total / cantidad
    } else 0L

    val endAvgChars = System.currentTimeMillis()
    val avgCharsSeconds = (endAvgChars - startAvgChars) / 1000.0

    // Imprimimos las estadísticas de descarga
    println(Formatters.formatProcessingStats(Map(
      "feedsSuccess"  -> feedsSuccessValue,
      "feedsFailed"   -> feedsFailedValue,
      "postsSuccess"  -> postsSuccessValue,
      "postsFailed"   -> postsFailedValue,
      "postsFiltered" -> postsFilteredValue,
      "avgChars"      -> avgChars
    )))

    // Verificamos si no hay mas posts que procesar
    if (filteredCount == 0) {
      println("Error: No valid posts downloaded after filtering")
      postsRDD.unpersist()
      filteredPostsRDD.unpersist()
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
      try {
        val combinedText = post.title + " " + post.selftext
        Analyzer.detectEntities(combinedText, dictionary)
      } catch {
        // Si lanza alguna excepcion
        case e: Exception =>
          println(s"Warning: Failed to analyze post '${post.title}': ${e.getMessage}")
          List.empty[NamedEntity]
      }
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
    val startCollectEntities = System.currentTimeMillis()
    val resultados = conteosRDD.collect()
    val endCollectEntities = System.currentTimeMillis()
    filteredPostsRDD.unpersist()
    val collectEntitiesSeconds = (endCollectEntities - startCollectEntities) / 1000.0

    // --- PASO 11: Formatear e imprimir (driver) ---
    // Ordenamos: primero por conteo descendente, luego por tipo, luego por nombre
    val ordenados = resultados
      .sortBy { case ((tipo, nombre), conteo) => (-conteo, tipo, nombre) }
      .take(cmdArgs.topK)

    // Imprimimos el ranking de entidades
    println(Formatters.formatEntityStats(resultados.toMap, cmdArgs.topK))
    // Estadisticas de tiempo de posts y feeds
    println(Formatters.formatTimingStats(
      totalPostsSeconds,
      filteredStageSeconds,
      avgCharsSeconds,
      collectEntitiesSeconds
    ))

    // Cerramos la SparkSession al terminar
    spark.stop()
  }
}
