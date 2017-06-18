import java.util.Calendar

import org.apache.spark.sql.SparkSession

/**
  * Created by utad on 6/17/17.
  */
object ModeloInferenciaEtiquetas {
  def main(args: Array[String]) {

    val directorio = args.toList match {
      case Nil => "hdfs://master.spark.tfm:9000/user/utad/"
      case arg :: Nil => if (arg.last == '/') arg else arg + "/"
      case _ => println("Uso: spark-submit \\\n  --class ModeloInferenciaEtiquetas \\\n  --master url-master \\\n  url-jar/sparkinferenciaetiquetas_2.11-1.0.jar [directorio]"); System.exit(1)
    }

    println("Directorio resultados: " + directorio)
    println("Inicio Proceso: " + Calendar.getInstance.getTime.toString)
    val spark = SparkSession
      .builder()
      .appName("Modelo Inferencia Etiquetas")
      .getOrCreate()

    // Leemos puntuaciones de inception generadas por proceso lanzado previamente
    println("Leemos y filtramos Etiquetas Inception: " + Calendar.getInstance.getTime.toString)
    val scoresInceptionDS = spark.read.json(directorio + "inception/clasification/part*").as[ScoresInception]

    val labelInception = scoresInceptionDS.map(clas => ScoresInception(clas.image.split("im")(1), clas.label, clas.score))

    println("Leemos y distribuimos las 1000 etiquetas de ImageNet: " + Calendar.getInstance.getTime.toString)
    val etqtasInception = spark.read.text(directorio + "inception/labels").collect()

    spark.sparkContext.broadcast(etqtasInception)

    // Leemos lista de imagenes de MIRFLICKR y sus etiquetas
    println("Escribimos imagenes-etiquetas de MIRFLICKR: " + Calendar.getInstance.getTime.toString )
    val etiquetasMIRFLICKR = spark.read.json(directorio + "mirflickr/labels-images").as[EtqtasMIRFLICKR].cache()


    spark.stop
  }
}
