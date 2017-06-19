import java.util.Calendar

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{CountVectorizer, IndexToString, StringIndexer}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by utad on 6/17/17.
  * Algoritmo Random Forest para predecir etiquetas de InceptionV3 desde Flickr
  */
object RFFlickrtoInception {
  def main(args: Array[String]) {

    val directorio = args.toList match {
      case Nil => "hdfs://master.spark.tfm:9000/user/utad/"
      case arg :: Nil => if (arg.last == '/') arg else arg + "/"
      case _ => println("Uso: spark-submit \\\n  --class ModeloInferenciaEtiquetas \\\n  --master url-master \\\n  url-jar/sparkinferenciaetiquetas_2.11-1.0.jar [directorio]"); System.exit(1)
    }

    println("Directorio lectura/resultados: " + directorio)
    println("Inicio Proceso: " + Calendar.getInstance.getTime.toString)
    val spark = SparkSession
      .builder()
      .appName("Modelo Inferencia Etiquetas")
      .getOrCreate()

    import spark.implicits._

    // Leemos puntuaciones de inception generadas por proceso lanzado previamente
    // Transformamos tupla de ("im999", "label", 0.6555) a ("999", label) ó ("999", label)
    println("Leemos y transformamos Etiquetas Inception: " + Calendar.getInstance.getTime.toString)
    val labelInceptionDF = spark.read.json(directorio + "inception/clasification/part*")
        .map(cl => EtiquetaOrigen(cl.getString(0).split("im")(1).toLong, cl.getString(1)))

    labelInceptionDF.show(false)

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

    trainingData.cache

    // Preparamos modelo creando un índice de categorías y su traducción inversa
    println("Preparamos modelo: " + Calendar.getInstance.getTime.toString )
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(joinDF)

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)


    println("Entrenamos Modelo: " + Calendar.getInstance.getTime.toString )
    // Entrenamos modelo
    val rfc = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("caracteristicas")
      .setNumTrees(30)

    val pipeline = new Pipeline().setStages(Array(labelIndexer, rfc, labelConverter))

    val modelRF = pipeline.fit(trainingData)

    // Hacemos predicciones
    println("Hacemos predicciones: " + Calendar.getInstance.getTime.toString )
    val predicciones = modelRF.transform(testData)

    // Mostramos y escribimos a disco las predicicones
    println("Mostramos y escribimos a disco predicicones: " + Calendar.getInstance.getTime.toString )
    predicciones.select("id", "label", "predictedLabel").show(false)
    predicciones.select("id", "label", "predictedLabel").coalesce(6).write.mode(SaveMode.Overwrite).json(directorio + "inception/predicciones")

    // Salvamos modelo
    println("Salvamos modelo: " + Calendar.getInstance.getTime.toString )
    modelRF.write.overwrite.save(directorio + "modelos")

    //Probamos modelo
    println("Probamos modelo: " + Calendar.getInstance.getTime.toString )
    val evaluador = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluador.evaluate(predicciones)

    println("\n*******************************")
    println("Test Error = " + (1.0 - accuracy))
    println("*******************************\n")

    spark.stop
  }
}
