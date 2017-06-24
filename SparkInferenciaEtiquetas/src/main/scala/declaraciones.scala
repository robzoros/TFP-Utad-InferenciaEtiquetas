import org.apache.spark.ml.linalg.Vector
/**
  * Created by utad on 6/18/17.
  */

// Definimos clases para los datasets
case class EtqtasMIRFLICKR(image: String, label_normalized: String)
case class ScoresInception(image: String, label: String, score: Double)
case class EtiquetaImagen(id: Long, label: String)
case class EtiquetaOrigenAgr(image: Long, labels: Array[String])
case class Clasificacion(id: Long, label: Long, features: Vector)
case class InceptionVector(image: String, labels: Array[Int], scores: Array[Double])

// Definimos funciones genéricas

object ExtraerNombreFicheros {
  // Función que extrae el nombre de la imagen de la ruta del fichero
  def getNombreImagenTags(fichero: String): String = fichero.split("/").last.split("\\.").head.split("tags").last

  //Función que extrae el número de imagen de un nombre de fichero "im999"
  def toLongNombreImagen(fichero: String): Long = fichero.split("im")(1).toLong
}
