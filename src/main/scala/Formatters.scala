object Formatters {

  /**
   * Format processing statistics for console output.
   * @param stats map with keys: "feedsSuccess", "feedsFailed", "postsSuccess", "postsFailed", "postsFiltered", "avgChars"
   * @return formatted statistics string
   */
  def formatProcessingStats(stats: Map[String, Long]): String = {
    val feedsSuccess = stats.getOrElse("feedsSuccess", 0L)
    val feedsFailed = stats.getOrElse("feedsFailed", 0L)
    val postsSuccess = stats.getOrElse("postsSuccess", 0L)
    val postsFailed = stats.getOrElse("postsFailed", 0L)
    val postsFiltered = stats.getOrElse("postsFiltered", 0L)
    val avgChars = stats.getOrElse("avgChars", 0L)

    s"""============ ESTADÍSTICAS DE PROCESAMIENTO ============
Feeds descargados exitosamente: $feedsSuccess
Feeds fallidos: $feedsFailed
Posts descargados exitosamente: $postsSuccess
Posts fallidos: $postsFailed
Posts filtrados (vacíos/nulos): $postsFiltered
Posts validos: ${postsSuccess - postsFiltered}
Largo promedio en posts: $avgChars"""
  }

  def formatTimingStats(
    totalPostsSeconds: Double,
    filteredStageSeconds: Double,
    avgCharsSeconds: Double,
    collectEntitiesSeconds: Double
  ): String = {
    s"""============ ESTADÍSTICAS DE TIEMPOS ============
Tiempo de descarga de posts: $totalPostsSeconds segundos
Tiempo de filtrado de posts: $filteredStageSeconds segundos
Tiempo de cálculo de promedio de caracteres: $avgCharsSeconds segundos
Tiempo de cálculo de entidades: $collectEntitiesSeconds segundos"""
  }

  /**
   * Format entity type statistics for console output.
   * @param typeStats map from entityType to count (including "total" key)
   * @return formatted entity statistics string
   */
  def formatTypeStats(typeStats: Map[String, Int]): String = {
    val total = typeStats.getOrElse("total", 0)
    val entityTypes = List("Person", "Organization", "University", "Place", "Technology", "ProgrammingLanguage")
    val typeLines = entityTypes.map { entityType =>
      val count = typeStats.getOrElse(entityType, 0)
      s"    [$entityType]: $count"
    }.mkString("\n")

    s"""============ ESTADÍSTICAS DE ENTIDADES ============
Entidades totales: $total
Entidades por categoría:
$typeLines"""
  }

  /**
   * Format entity statistics for console output.
   * Sorts by count (descending), then type (alphabetical), then name (alphabetical).
   * @param entityCounts map from (entityType, entityName) to count
   * @param topK number of top entities to display (default: 10)
   * @return formatted entity statistics string
   */
  def formatEntityStats(entityCounts: Map[(String, String), Int], topK: Int = 10): String = {
    val sorted = entityCounts
      .toList
      .sortBy { case ((entityType, entityName), count) =>
        (-count, entityType, entityName)
      }
      .take(topK)

    val formatted = sorted.map { case ((entityType, entityName), count) =>
      s"[Type=$entityType] $entityName: $count apariciones"
    }.mkString("\n")

    s"""============ ENTIDADES NOMBRADAS MÁS FRECUENTES ============
$formatted"""
  }
}
