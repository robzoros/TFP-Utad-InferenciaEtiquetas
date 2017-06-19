import java.util.Calendar

import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

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
      println("Uso: spark-submit \\\n  --class ModeloInferenciaEtiquetas \\\n  --master url-master \\\n  url-jar/sparkinferenciaetiquetas_2.11-1.0.jar [directorio]")
      System.exit(1)
    }

    println("Directorio lectura/resultados: " + directorio)
    println("Inicio Proceso: " + Calendar.getInstance.getTime.toString)
    val spark = SparkSession
      .builder()
      .appName("Inferencia Incepcyion desde Flickr")
      .getOrCreate()

    import spark.implicits._

    /**
      *
      * @param etiqueta: Etiqueta a Entrena
      * @return Un modelo entrenado de decision tree para la etiqueta
      */
    def entrenarModelo(etiqueta: String) : ( String, DecisionTreeClassificationModel ) = {

      println("\n**************************************************************")
      println(" Entrenamos etiqueta: " + etiqueta)
      println("**************************************************************\n")

      // Leemos puntuaciones de inception generadas por proceso lanzado previamente
      // Transformamos tupla de ("im999", "label", 0.6555) a ("999", label) ó ("999", label)
      println("Leemos, filtramos y transformamos Etiquetas Inception: " + Calendar.getInstance.getTime.toString)
      val labelInceptionDF = spark.read.json(directorio + "inception/clasification/part*")
        .map(cl => EtiquetaOrigen(cl.getString(0).split("im")(1).toLong, cl.getString(1)))
        .groupBy("id")
        .agg(collect_list("label") as "labels")
        .map(r => Clasificacion(r.getLong(0), r.getSeq(1).toArray[String].filter(_ == etiqueta).length))

      println(labelInceptionDF.count)
      println(labelInceptionDF.filter("label == 1").count)
      labelInceptionDF.filter("label == 1").show(false)

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

      joinDF.show(false)

      // Extraemos características con CountVectorizer
      println("Extraemos Características: " + Calendar.getInstance.getTime.toString )
      val datos = new CountVectorizer()
        .setInputCol("labels")
        .setOutputCol("caracteristicas")
        .setVocabSize(10000)
        .fit(joinDF)
        .transform(joinDF)

      datos.show()

      // Dividimos datos para entrenar y probar.
      println("Dividimos datos para entrenar y probar.: " + Calendar.getInstance.getTime.toString )
      val Array(trainingData, testData) = datos.select("id", "label", "caracteristicas").randomSplit(Array(0.7, 0.3))

      println("Entrenamos Modelo: " + Calendar.getInstance.getTime.toString )
      // Entrenamos modelo
      val dtc = new DecisionTreeClassifier()
        .setLabelCol("label")
        .setFeaturesCol("caracteristicas")
        .setImpurity("gini")
        .setMaxDepth(8)

      val modelDT = dtc.fit(trainingData)

      // Hacemos predicciones
      println("Hacemos predicciones: " + Calendar.getInstance.getTime.toString )
      val predicciones = modelDT.transform(testData)

      // Mostramos y escribimos a disco las predicicones
      println("Mostramos y escribimos a disco predicicones: " + Calendar.getInstance.getTime.toString )
      predicciones.select("id", "label", "prediction").show(false)
      predicciones.select("id", "label", "prediction").coalesce(6).write.mode(SaveMode.Overwrite).json(directorio + "inception/predicciones")

      // Salvamos modelo
      println("Salvamos modelo: " + Calendar.getInstance.getTime.toString )
      modelDT.write.overwrite.save(directorio + "modelos")

      //Probamos modelo
      println("Probamos modelo: " + Calendar.getInstance.getTime.toString )
      val evaluador = new MulticlassClassificationEvaluator()
        .setLabelCol("label")
        .setPredictionCol("prediction")
        .setMetricName("accuracy")
      val accuracy = evaluador.evaluate(predicciones)

      println("\n*******************************")
      println("Test Error = " + (1.0 - accuracy))
      println("*******************************\n")

      (etiqueta, modelDT)
    }

    // Leemos etiquetas más comunes de Inception y las distribuimos con un RDD
    println("Leemos etiquetas más comunes de Inception: " + Calendar.getInstance.getTime.toString)
    //val modelosEntrenados = spark.sparkContext.textFile(directorio + "inception/comunes/part*").collect().map(entrenarModelo)

    val l = Seq("comic book").map(entrenarModelo)

    println(l(0)._1)


  }

}
