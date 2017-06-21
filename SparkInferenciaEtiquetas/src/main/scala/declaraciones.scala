/**
  * Created by utad on 6/18/17.
  */

// Definimos clases para los datasets
case class EtqtasMIRFLICKR(image: String, label_normalized: String)
case class ScoresInception(image: String, label: String, score: Double)
case class EtiquetaOrigen(id: Long, label: String)
case class EtiquetaOrigenAgr(image: Long, labels: Array[String])
case class Clasificacion(id: Long, label: Long)
case class InceptionVector(image: String, labels: Array[Int], scores: Array[Double])

