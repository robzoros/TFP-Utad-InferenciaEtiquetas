import java.util.Calendar

import ExtraerNombreFicheros.toLongNombreImagen
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by utad on 6/17/17.
  * Algoritmo para inferencia de etiquetas de InceptionV3 desde Flickr
  */
object InfFlickrtoInception {

  def main(args: Array[String]) {

    val directorio = args.toList match {
      case Nil => "hdfs://master.spark.tfm:9000/user/utad/"
      case arg :: Nil => if (arg.last == '/') arg else arg + "/"
      case _ => Nil
    }

    if (directorio == Nil) {
      println("Uso: spark-submit \\\n  --class InfFlickrtoInception \\\n  --master url-master \\\n  url-jar/sparkinferenciaetiquetas_2.11-1.0.jar [directorio]")
      System.exit(1)
    }

    println("Directorio lectura/resultados: " + directorio)
    println("Inicio Proceso: " + Calendar.getInstance.getTime.toString)
    val spark = SparkSession
      .builder()
      .appName("Inferencia Incepction desde Flickr")
      .getOrCreate()

    import spark.implicits._

    // Leemos puntuaciones de inception generadas por proceso lanzado previamente
    // Transformamos tupla de ("im999", "label", 0.6555) a ("999", label)
    println("Leemos, filtramos y transformamos Etiquetas Inception: " + Calendar.getInstance.getTime.toString)
    val labelInceptionDF = spark.read.json(directorio + "inception/classification/")
      .map(cl => EtiquetaImagen(toLongNombreImagen(cl.getString(0)), cl.getString(1)))
      .groupBy("id")
      .agg(collect_list("label") as "classification")


    // Leemos lista de imagenes de MIRFLICKR y sus etiquetas
    println("Leemos imagenes-etiquetas de MIRFLICKR: " + Calendar.getInstance.getTime.toString )
    val etiquetasMIRFLICKRDF = spark.read.json(directorio + "mirflickr/labels-images")
      .groupBy("image")
      .agg(collect_list("label_normalized") as "labels")
      .map(r => EtiquetaOrigenAgr(r.getString(0).toLong, r.getSeq(1).toArray))

    println("Join entre Inception y MIRFLICKR: " + Calendar.getInstance.getTime.toString )
    // Row(id   |label|image|labels )
    val joinDF = labelInceptionDF
      .join(etiquetasMIRFLICKRDF, labelInceptionDF("id") === etiquetasMIRFLICKRDF("image"))

    joinDF.show()

    // Extraemos características con CountVectorizer (las 10.000 más comunes)
    println("Extraemos Características: " + Calendar.getInstance.getTime.toString )
    val datos = new CountVectorizer()
      .setInputCol("labels")
      .setOutputCol("features")
      .setVocabSize(10000)
      .fit(joinDF)
      .transform(joinDF)

    // Dividimos datos para entrenar y probar.
    println("Dividimos datos para entrenar y probar.: " + Calendar.getInstance.getTime.toString )
    val Array(trainingData, testData) = datos.select("id", "classification", "features").randomSplit(Array(0.8, 0.2))

    /**
      *
      * @param etiqueta: Etiqueta a Entrenar
      * @return Un modelo entrenado de decision tree para la etiqueta
      */
    def entrenarModelo(etiqueta: String) : ( String, DecisionTreeClassificationModel ) = {

      println("\n**************************************************************")
      println(" Entrenamos etiqueta: " + etiqueta)
      println("**************************************************************\n")

      // Añadimos columna con el valor para la etiqueta que vamos a entrenar
      // Pasamos de ("id", "classification", "features") a ("id", "label", "features")
      val trainingDataEtiqueta = trainingData
        .map(r => Clasificacion(r.getLong(0), r.getSeq(1).toArray[String].count(_ == etiqueta), r.getAs("features")))

      println(trainingDataEtiqueta.count)
      println(trainingDataEtiqueta.filter("label == 1").count)
      println(trainingDataEtiqueta.filter("label == 0").count)

      println("Entrenamos Modelo: " + Calendar.getInstance.getTime.toString )
      // Entrenamos modelo
      val dtc = new DecisionTreeClassifier()
        .setLabelCol("label")
        .setFeaturesCol("features")
        .setImpurity("gini")
        .setMaxDepth(8)

      val modelDT = dtc.fit(trainingDataEtiqueta)

      (etiqueta, modelDT)
    }
    /** Fin  entrenarModelo              */
    /*************************************/

    // Obtenemos una lista de modelos entrenados para cada etiqueta (etiqueta, modelo)
    println("Leemos etiquetas y entrenamos: " + Calendar.getInstance.getTime.toString)
    val listaModelosEntrenados = spark.sparkContext.textFile(directorio + "inception/comunes/").collect.map(entrenarModelo)

    // Hacemos Predicciones con el set de imágenes del trainData para cada modelo
    println("Hacemos predicciones: " + Calendar.getInstance.getTime.toString )
    val predicciones = listaModelosEntrenados
      .map(modelo => modelo._2.transform(testData).filter("prediction == 1").map(fila => EtiquetaImagen(fila.getLong(0), modelo._1) ))
      .reduce(_.union(_))

    predicciones.show(false)
    predicciones.coalesce(6).write.mode(SaveMode.Overwrite).json(directorio + "inception/predicciones/")

  }

}
