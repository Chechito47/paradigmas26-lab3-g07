import scala.io.Source
import org.json4s._
import org.json4s.jackson.JsonMethods._

object FileIO {

  /**
   * Leer suscripciones de un archivo JSON
   * Imprime un warning por cada suscripcion mal formada y la skipea
   * @param filePath path al subscriptions file
   * @return Left(errorMessage) si el archivo no existe o el JSON es no es valido
   *         Right(suscripciones) con las suscripciones validas
   *         Puede devolver vacio
   */
  def readSubscriptions(filePath: String): Either[String, List[Subscription]] = {
    implicit val formats: Formats = DefaultFormats

    // Intentamos leer el archivo. Si no existe devolvemos left con el mensaje de error
    val content = try {
      val source = Source.fromFile(filePath)
      val text = source.mkString
      source.close()
      text
    } catch {
      // El archivo no existe o no se puede leer devuelvo Left
      case _: java.io.FileNotFoundException =>
        return Left(s"Error: Could not load $filePath - file not found")
    }

    // Intentamos parsear el JSON
    val rawList: List[Map[String, String]] = try {
      parse(content).extract[List[Map[String, String]]]
    } catch {
      //Si el JSON es invalido devuelvo Left
      case _: Exception =>
        return Left(s"Error: Could not load $filePath - invalid JSON format")
    }

    // Procesamos cada entrada: si le falta name o url, imprimimos warning y la salteamos
    // get("name") devuelve Option[String]: Some si existe, None si no
    val subscriptions = rawList.flatMap { sub =>
      (sub.get("name"), sub.get("url")) match {
        // Caso entrada valida
        case(Some(name), Some(url)) =>
          Some(Subscription(name, url))
        // Cualquier otro caso => descartado (falta un campo)
        case _ =>
          println("Warning: Skipping malformed subscription (missing 'name' or 'url' field)")
          None
    }
  }
  Right(subscriptions)
}

  /**
   * Descargar feed JSON de una URL
   * Imprime un warning con el nombre de la suscripcion si la descarga falla
   * @param url Reddit feed URL, subscriptionName
   * @return Option Some(Content)(JSON as String), None si hay error de red o timeout
   */
  def downloadFeed(url: String, subscriptionName: String): Option[String] = {
    try {
      val source = Source.fromURL(url)
      val content = source.mkString
      source.close()
      Some(content)
    } catch {
      // Error de red, timeout
      case _: Exception =>
        None
    }
  }

  /**
   * Lee el archivo de diccionario linea por linea
   * @param filePath path al dictionary file
   * @return Option con lista de entidades, None si el archivo no existe
   */
  def readDictionaryFile(filePath: String): Option[List[String]] = {
    try {
      val source = Source.fromFile(filePath)
      val lines = source.getLines()
        .map(_.trim)
        .filter(_.nonEmpty)
        .filterNot(_.startsWith("#"))
        .toList
      source.close()
      Some(lines)
    } catch {
      // Archivo no existe
      case _: Exception =>
        None
    }
  }
}
