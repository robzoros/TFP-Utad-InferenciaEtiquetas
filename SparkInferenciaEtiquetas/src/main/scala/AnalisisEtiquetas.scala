import ExtraerNombreFicheros.getNombreImagenTags
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by Roberto on 07/06/17.
  */
object AnalisisEtiquetas {
  // Etiquetas elegidas después de analizar salida de anteriores ejecuciones
  val etiquetasNoSignificativas = Array("explore", "canon", "2007", "nikon", "mm", "50", "macro", "de", "365", "500", "2008", "photo", "100", "10", "40", "geotagged", "eo", "la", "san", "flickr", "400", "explored", "80", "200", "f1", "geolat", "geolon", "diamondclassphotographer", "photography", "hdr", "f2", "30", ".8", "07", "ef")

  def main(args: Array[String]) {

    val directorio  = args.toList match {
      case Nil => "hdfs://master.spark.tfm:9000/user/utad/"
      case arg :: Nil => if (arg.last == '/') arg else arg + "/"
      case _ => println("Uso: spark-submit \\\n  --master url-master \\\n  url-jar/sparkinferenciaetiquetas_2.11-1.0.jar [directorio]"); System.exit(1)
    }

    println("Directorio resultados: " + directorio)
    val spark = SparkSession
      .builder()
      .appName("Analisis Etiquetas")
      .getOrCreate()

    import spark.implicits._

    // *****************************************
    // Etiquetas MIRFLICKR
    // *****************************************

    // Leemos etiquetas del data set de MIRFLICKR (guardada en unidad compartida a la que los workers tienen acceso)
    val etiquetasMF = spark.read.text("file:/mnt/hgfs/TFM/mirflickr/meta/tags_raw/")
      .select(input_file_name.alias("image"), $"value".alias("label"))

    val etiquetasRDD = etiquetasMF.rdd.map(row => (getNombreImagenTags(row.getString(0)), row.getString(1))).cache

    // Obtenemos y lo guardamos en disco idioma de cada etiqueta según lo identifica langid (https://github.com/saffsd/langid.py)
    etiquetasRDD.map(tupla => tupla._2)
      .pipe("/mnt/hgfs/TFM/Language.py")
      .toDF("idioma")
      .groupBy("idioma").count()
      .coalesce(3).write.mode(SaveMode.Overwrite).csv(directorio + "mirflickr/idiomas")

    // Convertimos en Tokens la etiqueta
    val etiquetasMFDF = etiquetasRDD.map(tupla => (tupla._1, Normalizador.tokenizer(tupla._2) ))
      .toDF("image", "tokens")

    // StopWordsRemover en inglés
    val remover = new StopWordsRemover()
      .setInputCol("tokens")
      .setOutputCol("clean_tokens")

    // Dataset con tuplas (image, label_normalized)
    val etiquetasDS = remover.transform(etiquetasMFDF)
      .flatMap( row => row.getAs[Seq[String]]("clean_tokens")
        .map( token => EtqtasMIRFLICKR(row.getString(0), token))
      )
      .as[EtqtasMIRFLICKR]
      .distinct
      .cache

    // Etiquetas Más comunes: Obtenemos las 50 más comunes después de eliminar tokens no significativos
    // Mostramos y escribimos a disco
    val etqtasAgrMFDS = etiquetasDS.groupBy("label_normalized").count.cache

    val etqtasMasComunesMFDS = etqtasAgrMFDS
      .orderBy($"count".desc)
      .limit(100)
      .filter(r => ! etiquetasNoSignificativas.contains(r.getString(0)))
      .select("label_normalized")
      .limit(50)
      .orderBy($"label_normalized")

    etqtasMasComunesMFDS.write.mode(SaveMode.Overwrite).text(directorio + "mirflickr/comunes")

    // Escribimos a disco todas las etiquetas (coalesce(6) para tener seis particiones y no doscientas)
    val etqtasTodasMFDS = etqtasAgrMFDS.select("label_normalized")
    etqtasTodasMFDS.coalesce(6).write.mode(SaveMode.Overwrite).text(directorio + "mirflickr/labels")

    // Escribimos en HDFS para posteriores procesos la lista de imagenes y sus etiquetas (reducimos los 25000 ficheros de entrada a 6 ficheros)
    etiquetasDS.map(l => EtiquetaImagen(l.image.toLong, l.label_normalized))
      .orderBy("id")
      .coalesce(6).write.mode(SaveMode.Overwrite).json(directorio + "mirflickr/labels-images")

    // Escribimos a disco todas las etiquetas con su cuenta para posterior análisis
    etqtasAgrMFDS.coalesce(6).write.mode(SaveMode.Overwrite).option("delimiter", "~").csv(directorio + "mirflickr/analisis")

    // Escribimos a disco las etiquetas originales en formato JSON
    etiquetasRDD.map(fila => (fila._1.toLong, fila._2))
      .toDF("id", "label")
      .as[EtiquetaImagen]
      .orderBy("id")
      .coalesce(6).write.mode(SaveMode.Overwrite).json(directorio + "mirflickr/original")

    // *****************************************
    // Categorización InceptionV3(TensorFlow)
    // *****************************************

    // Leemos puntuaciones de inception generadas por proceso lanzado previamente
    val scoresInceptionDS = spark.read.json(directorio + "inception/classification/").as[ScoresInception].cache()

    // Etiquetas Más comunes: Obtenemos las 50 más comunes, mostramos y escribimos a disco
    val etqtasCuentaTodasInception = scoresInceptionDS
      .filter("label <> '__None__'") //filtamos imágenes sin etiquetas
      .groupBy("label")
      .count.cache

    val etqtasMasComunesIDS = etqtasCuentaTodasInception
      .orderBy($"count".desc)
      .limit(50)
      .select("label")
      .orderBy($"label")

    etqtasMasComunesIDS.show()

    etqtasMasComunesIDS.write.mode(SaveMode.Overwrite).text(directorio + "inception/comunes")

    // Todas las etiquetas (coalesce(6) para tener seis particiones y no doscientas)
    val etqtasTodasInception = etqtasCuentaTodasInception.select("label")
    etqtasTodasInception.coalesce(6).write.mode(SaveMode.Overwrite).text(directorio + "inception/labels")

    // Escribimos las imágenes que tienen etqueta __None__ por iamgen mal formada
    val noneImagesInception = scoresInceptionDS.filter("label == '__None__'").select("image")
    noneImagesInception.coalesce(3).write.mode(SaveMode.Overwrite).text(directorio + "inception/none")

    // Todas las etiquetas para analisis(coalesce(6) para tener seis particiones y no doscientas)
    etqtasCuentaTodasInception.coalesce(6).write.mode(SaveMode.Overwrite).option("delimiter", "~").csv(directorio + "inception/analisis")

    spark.stop()

  }

}