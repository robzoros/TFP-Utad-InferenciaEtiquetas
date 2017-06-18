import java.text.Normalizer.{normalize => jnormalize, _}

/**
  * Created by utad on 6/15/17.
  * Trait y Object creados para agrupar funciones que se utilizan a lo largo de la lectura y tratamiento de
  * las etiquetas que los usuarios han puesto a sus fotos en Flickr.
  */

trait Normalizador {

  // En esta función limpiamos la entrada para agrupar determinados elementos que al pasar por
  // el "tokenizador" de nltk no se queden separados.
  // Geolocalizacion:
  //   Muchas fotos estan geolocalizadas con etiquetas de latitud y longitud
  //   de la forma geo:lat=51.509806 y geo:lon=-0.076617
  //   las convertiremos en etiquetas geolat51 y geolon-0 para agrupar las etiquetas
  //   por cuadrantes.
  // Símbolos
  //   El simbolo & viene en las etiquetas como &amp; lo dejamos como &
  def limpiarEntrada(input: String) : String = {
    val patronNumero = "(.*)(geo:)(lat|lon)(=)(-?[0-9]+?)(\\.[0-9]{4,})(.*)".r

    input.replaceAll("&amp;", "&") match {
      case patronNumero(r1, _, latlong, _, numero, _, r2) => r1 + "geo" + latlong + numero + r2
      case cadena => cadena
    }
  }

  // Hacemos una separación de palabras con caracteres latinos
  // Cogemos datos de cámaras para tenerlos separados y
  // separamos números de palabras y palabras de guiones o puntos
  // mantenemos números, puntos y guiones juntos (2.2.2007 por ejemplo)
  def retokenize(palabra: String) : List[String] = {
    def retokenizeHelper (palabra: String) : List[String] = {

      val patCamara = "(.*)(canon)(.*)".r
      val patObjetivo = "([a-z]*)([0-9]{1,2}-[0-9]{2,3}mm)(.*)".r
      val patObjetivo2 = "([a-z]*)([0-9]{4,5}mm)(.*)".r
      val patFpunto = "(.*)(f[0-9]\\.[0-9])(.*)".r
      val patF = "(.*)(f[0-9]{1,5})(.*)".r
      val patLetraNum = "(^[a-z]+?)([0-9].*)".r
      val patNumLetra = "(^[0-9]+?)([a-z].*)".r
      val patLetraPunto = "(^[a-z]+?\\.)(.*)".r
      val patLetraGuion = "(^[a-z]+?\\-)(.*)".r
      val patPuntoLetra = "(^\\.+?[a-z])(.*)".r
      val patGuionLetra = "(^\\-+?[a-z])(.*)".r

      val result = palabra match {
        case patCamara(r1, camara, r2) => r1 + " " + camara + " " + r2
        case patObjetivo(camara, objetivo, resto) => camara + " " + objetivo + " " + resto
        case patObjetivo2(camara, objetivo, resto) => camara + " " + objetivo.slice(0,2) + "-" + objetivo.slice(2,10) + " " + resto
        case patFpunto(previo, numF, resto) => previo + " " + numF + " " + resto
        case patF(previo, numF, resto) => previo + " " + numF + " " + resto
        case patLetraNum(letras, resto) => letras + " " + resto
        case patNumLetra(numeros, resto) => numeros + " " + resto
        case patLetraPunto(_, _) => palabra.replaceAll("\\.", " ")
        case patLetraGuion(_, _) => palabra.replaceAll("\\-", " ")
        case patPuntoLetra(_, _) => palabra.replaceAll("\\.", " ")
        case patGuionLetra(_, _) => palabra.replaceAll("\\-", " ")
        case _ => palabra
      }
      result.trim.split(" ").toList.filter(_.length > 1)
    }

    retokenizeHelper(palabra) match {
      case Nil => Nil
      case w :: Nil => if (w == palabra) List(w) else retokenizeHelper(w).flatMap(retokenize)
      case w :: resto  => retokenize(w) ++ resto.flatMap(retokenize)
    }

  }

  def normalize(in: String): String = {
    try {
      val cleaned = in.trim.toLowerCase
      val normalized = jnormalize(cleaned, Form.NFD).replaceAll("[\\p{InCombiningDiacriticalMarks}\\p{IsM}\\p{IsLm}\\p{IsSk}]+", "")

      normalized.replaceAll("&amp;", "&")
        .replaceAll("&apos;", "")
        .replaceAll("&#124;","")
        .replaceAll("&#91;","")
        .replaceAll("&#93;","")
        .replaceAll("\\.\\.","")
        .replaceAll("--","-")
    }
    catch {
      case e: Exception => "ERROR: [" + in + "] " + e.printStackTrace()
    }
  }


}

object Normalizador extends Normalizador
