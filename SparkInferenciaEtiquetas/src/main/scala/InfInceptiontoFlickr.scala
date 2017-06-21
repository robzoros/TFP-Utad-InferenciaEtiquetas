import java.util.Calendar

import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by utad on 6/17/17.
  */
object InfInceptiontoFlickr {
  def main(args: Array[String]) {

    val directorio = args.toList match {
      case Nil => "hdfs://master.spark.tfm:9000/user/utad/"
      case arg :: Nil => if (arg.last == '/') arg else arg + "/"
      case _ => println("Uso: spark-submit \\\n  --class InfInceptiontoFlickr \\\n  --master url-master \\\n  url-jar/sparkinferenciaetiquetas_2.11-1.0.jar [directorio]"); System.exit(1)
    }

    println("Directorio resultados: " + directorio)
    println("Inicio Proceso: " + Calendar.getInstance.getTime.toString)
    val spark = SparkSession
      .builder()
      .appName("Modelo Inferencia Etiquetas")
      .getOrCreate()

    import spark.implicits._

    /**
      *
      * @param etiqueta: Etiqueta a Entrenar
      * @return Un modelo entrenado de decision tree para la etiqueta
      */
    def entrenarModelo(etiqueta: String) : ( String, DecisionTreeClassificationModel ) = {

      println("\n**************************************************************")
      println(" Entrenamos etiqueta: " + etiqueta)
      println("**************************************************************\n")

      // Leemos etiqueta de Flickr
      println("Leemos y filtramos y transformamos Etiquetas Flickr: " + Calendar.getInstance.getTime.toString)
      val labelFlickrDF = spark.read.json(directorio + "mirflickr/labels-images/")
        .groupBy("image")
        .agg(collect_list("label_normalized") as "labels")
        .map(r => Clasificacion(r.getString(0).toLong, r.getSeq(1).toArray[String].count(_ == etiqueta)))

      println(labelFlickrDF.count)
      println(labelFlickrDF.filter("label == 1").count)
      labelFlickrDF.filter("label == 1").show(false)

      // Leemos lista de imagenes de Inception y todas las etiquetas de inception que guardamos en un array
      println("Leemos imagenes-etiquetas de Inception: " + Calendar.getInstance.getTime.toString )
      val scoresInceptionDS = spark.read.json(directorio + "inception/clasification/").as[ScoresInception]
      val labelsInception = scoresInceptionDS.select("label").distinct.sort("label").collect()

      val cuentaEtiquetas = labelsInception.length
      println("Cuenta Etiquetas: " + cuentaEtiquetas)

      // mandamos a los workers el array y la cuenta de etiquetas
      spark.sparkContext.broadcast(labelsInception)
      spark.sparkContext.broadcast(cuentaEtiquetas)

      // Transformamos clasificación de inception en Vector
      println("Leemos imagenes-etiquetas de Inception: " + Calendar.getInstance.getTime.toString )
      val labelInceptionRDD = scoresInceptionDS.groupBy("image")
        .agg(collect_list("label") as "labels", collect_list("score") as "scores")
        .rdd
        .map(fila => (fila.getString(0), fila.getAs[Seq[String]]("labels").sorted, fila.getSeq(2)))

      val labelCaracteristicas  = labelInceptionRDD
        .map { case (imagen, labels, scores) =>
          (imagen.split("im")(1).toLong, Vectors.sparse(cuentaEtiquetas, labels.map(labelsInception.indexOf).toArray, scores.toArray) )
        }
        .toDF("imagen", "caracteristicas")

      labelCaracteristicas.show()


      // Hacemos join entre los dos DataFrames
      println("Join entre Inception y MIRFLICKR: " + Calendar.getInstance.getTime.toString )
      // Row(id   |label|image|labels )
      val joinDF = labelFlickrDF
        .join(labelCaracteristicas, labelFlickrDF("id") === labelCaracteristicas("imagen"))

      joinDF.show(false)


      // Dividimos datos para entrenar y probar.
      println("Dividimos datos para entrenar y probar.: " + Calendar.getInstance.getTime.toString )
      val Array(trainingData, testData) = joinDF.select("id", "label", "caracteristicas").randomSplit(Array(0.7, 0.3))

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

    val l = Seq("sky").map(entrenarModelo)

    println(l.head._1)


  }
}
