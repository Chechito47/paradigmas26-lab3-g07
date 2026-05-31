import scala.io.Source
import org.json4s._
import org.json4s.jackson.JsonMethods._

object FileIO {

  /**
   * Leer suscripciones de un archivo JSON
   * Devuelve Left(errorMessage) si el archivo no existe o el JSON es no es valido
   * Devuelve Right(suscripciones) con las suscripciones validas (puede ser vacio)
   * Imprime un warning por cada suscripcion mal formada y la skipea
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
   * Download feed JSON from URL.
   * @param url Reddit feed URL
   * @return Option containing JSON as String, None on network error or timeout
   */
  def downloadFeed(url: String): Option[String] = {
    val source = Source.fromURL(url)
    val content = source.mkString
    source.close()
    Some(content)
  }

  /**
   * Read dictionary file line by line.
   * @param filePath path to dictionary file
   * @return Option containing list of entities, None if file missing
   */
  def readDictionaryFile(filePath: String): Option[List[String]] = {
    val source = Source.fromFile(filePath)
    val lines = source.getLines()
      .map(_.trim)
      .filter(_.nonEmpty)
      .filterNot(_.startsWith("#"))
      .toList
    source.close()
    Some(lines)
  }
}
