import java.text.Normalizer.{normalize, _}


/**
  * Created by utad on 6/15/17.
  * Trait y Object creados para agrupar funciones que se utilizan a lo largo de la lectura y tratamiento de
  * las etiquetas que los usuarios han puesto a sus fotos en Flickr.
  */

trait Normalizador {

  // En esta función limpiamos la entrada para agrupar elementos
  // Geolocalizacion:
  //   Muchas fotos estan geolocalizadas con etiquetas de latitud y longitud
  //   de la forma geo:lat=51.509806 y geo:lon=-0.076617
  //   las convertiremos en etiquetas geolat51 y geolon-0 para agrupar las etiquetas
  //   por cuadrantes.
  // Símbolos
  //   El simbolo & viene en las etiquetas como &amp; lo dejamos como &
  // Otros
  //   Hacemos una separación de palabras con caracteres latinos
  //   Cogemos datos de cámaras para tenerlos separados y
  //   separamos números de palabras y palabras de guiones o puntos
  //   mantenemos números, puntos y guiones juntos pero sustituimos
  //   . y / por guioned (de 2.2.2007 a 2-2-2007 por ejemplo)
  def tokenizer(input: String) : List[String] = {
    def tokenizerHelper (palabra: String) : List[String] = {

      val patGeolocalizacion = "(.*)(geo:)(lat|lon)(=)(-?[0-9]+?)(\\.[0-9]{4,})(.*)".r
      val patEmoticono = "(.*)(:|;-?)(\\)|\\(|P|D)(?![a-z])(.*)".r
      val patComaPunto = "(.*[0-9])(,)([0-9].*)".r
      val patF = "(.*)(f\\/?)([0-9]{1,5})(.*)".r
      val patFechaPunto = "(.*)([0-9]+)\\.([0-9]+)\\.([0-9]+)(.*)".r
      val patFechaSlash = "(.*)([0-9]+)\\/([0-9]+)\\/([0-9]+)(.*)".r
      val patCamara = "(.*)(canon)(.*)".r
      val patObjetivo = "([a-z]*)([0-9]{1,2}-[0-9]{2,3}mm)(.*)".r
      val patObjetivo2 = "([a-z]*)([0-9]{4,5}mm)(.*)".r
      val patFpunto = "(.*)(f[0-9]\\.[0-9])(.*)".r
      val patLetraNum = "(^[a-z]+?)([0-9].*)".r
      val patNumLetra = "(^[0-9]+?)([a-z].*)".r
      val patLetraPunto = "(^[a-z]+?\\.)(.*)".r
      val patLetraGuion = "(^[a-z]+?\\-)(.*)".r
      val patPuntoLetra = "(^\\.+?[a-z])(.*)".r
      val patGuionLetra = "(^\\-+?[a-z])(.*)".r


      val procesado = palabra match {
        case patGeolocalizacion(r1, _, latlong, _, numero, _, r2) => r1 + "geo" + latlong + numero + r2
        case patEmoticono(r1, _, _, r2) => r1 + "emoticono" + r2
        case patComaPunto(r1, r2) => r1 + "." + r2
        case patF(previo, _, numF, resto) => previo + " f" + numF + " " + resto
        case patFechaPunto(previo, f1, f2, f3, resto) => previo + f1 +"-" + f2 +"-" + f3 + resto
        case patFechaSlash(previo, f1, f2, f3, resto) => previo + f1 +"-" + f2 +"-" + f3 + resto
        case patCamara(r1, camara, r2) => r1 + " " + camara + " " + r2
        case patObjetivo(camara, objetivo, resto) => camara + " " + objetivo + " " + resto
        case patObjetivo2(camara, objetivo, resto) => camara + " " + objetivo.slice(0,2) + "-" + objetivo.slice(2,10) + " " + resto
        case patFpunto(previo, numF, resto) => previo + " " + numF + " " + resto
        case patLetraNum(letras, resto) => letras + " " + resto
        case patNumLetra(numeros, resto) => numeros + " " + resto
        case patLetraPunto(_, _) => palabra.replaceAll("\\.", " ")
        case patLetraGuion(_, _) => palabra.replaceAll("\\-", " ")
        case patPuntoLetra(_, _) => palabra.replaceAll("\\.", " ")
        case patGuionLetra(_, _) => palabra.replaceAll("\\-", " ")
        case palabra => palabra
      }

      procesado.replaceAll("&amp;", "&")
        .replaceAll("[\\[\\]\\?¿\\!¡_\\(\\)\\$\\+\\*\\/=:\\|\\\\,;#~]"," ")
        .replaceAll("\\.\\.","")
        .replaceAll("--","-")
        .replaceAll("\\s+", " ")
        .trim
        .split(" ")
        .toList
        .filter(_.length > 1)
    }

    try {
      val normalizado = normalize(input.trim.toLowerCase, Form.NFD).replaceAll("[\\p{InCombiningDiacriticalMarks}\\p{IsM}\\p{IsLm}\\p{IsSk}]+", "")

      tokenizerHelper(Inflector.singularize(normalizado)) match {
        case Nil => Nil
        case w :: Nil => if (w == input) List(w) else tokenizerHelper(w).flatMap(tokenizer)
        case w :: resto => tokenizer(w) ++ resto.flatMap(tokenizer)
      }
    }
    catch {
      case e: Exception => input.split(" ").toList
    }

  }

}

object Normalizador extends Normalizador
