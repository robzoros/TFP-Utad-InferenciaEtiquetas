import java.util.Calendar

import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.{SaveMode, SparkSession}


/**
  * Created by Roberto on 07/06/17.
  */
object AnalisisEtiquetas {

  def main(args: Array[String]) {

    val directorio  = args.toList match {
      case Nil => "hdfs://master.spark.tfm:9000/user/utad/"
      case arg :: Nil => if (arg.last == '/') arg else arg + "/"
      case _ => println("Uso: spark-submit \\\n  --master url-master \\\n  url-jar/sparkinferenciaetiquetas_2.11-1.0.jar [directorio]"); System.exit(1)
    }

    println("Directorio resultados: " + directorio)
    println("Inicio Proceso: " + Calendar.getInstance.getTime.toString )
    val spark = SparkSession
      .builder()
      .appName("Analisis Etiquetas")
      .getOrCreate()

    import spark.implicits._

    // *****************************************
    // Etiquetas MIRFLICKR
    // *****************************************

    // definimos función que extrae el nombre de la imagen de la ruta del fichero
    def getNombreImagen( fichero:String ) : String = fichero.split("/").last.split("\\.").head.split("tags").last

    // Leemos etiquetas del data set de MIRFLICKR (guardada en unidad compartida a la que los workers tienen acceso)
    println("Etiquetas MIRFLICKR: " + Calendar.getInstance.getTime.toString )
    val etiquetasMF = spark.read.text("file:/mnt/hgfs/TFM/mirflickr/meta/tags_raw/")
      .select(input_file_name.alias("image"), $"value".alias("label"))
    //val etiquetasRDD = etiquetasMF.rdd.map(row => getNombreImagen(row.getString(0)) + " " + Normalizador.limpiarEntrada(row.getString(1))).cache
    val etiquetasRDD = etiquetasMF.rdd.map(row => (getNombreImagen(row.getString(0)), row.getString(1))).cache

    // Convertimos en Tokens la etiqueta
    val etiquetasMFDS = etiquetasRDD.map(tupla => (tupla._1, Normalizador.tokenizer(tupla._2) ))
      .toDF("image", "tokens")

    val remover = new StopWordsRemover()
      .setInputCol("tokens")
      .setOutputCol("clean_tokens")

    val etiquetasDS = remover.transform(etiquetasMFDS)
      .flatMap( row => row.getAs[Seq[String]]("clean_tokens")
        .map( token => EtqtasMIRFLICKR(row.getString(0), token))
      )
      .as[EtqtasMIRFLICKR]
      .distinct
      .cache

    // Etiquetas Más comunes: Obtenemos las 50 más comunes, mostramos y escribimos a disco
    val etqtasAgrMFDS = etiquetasDS.groupBy("label_normalized").count.cache
    val etqtasMasComunesMFDS = etqtasAgrMFDS
      .orderBy($"count".desc)
      .limit(50)
      .select("label_normalized")
      .orderBy($"label_normalized")

    println("Mostramos Etiquetas MIRFLICKR: " + Calendar.getInstance.getTime.toString)
    etqtasMasComunesMFDS.show

    println("Escribimos Etiquetas más comunes MIRFLICKR: " + Calendar.getInstance.getTime.toString )
    etqtasMasComunesMFDS.write.mode(SaveMode.Overwrite).text(directorio + "mirflickr/comunes")

    // Escribimos a disco todas las etiquetas (coalesce(6) para tener seis particiones y no doscientas)
    val etqtasTodasMFDS = etqtasAgrMFDS.select("label_normalized")
    println("Cuenta Etiquetas MIRFLICKR " + etqtasTodasMFDS.count + " :" + Calendar.getInstance.getTime.toString)
    etqtasTodasMFDS.coalesce(6).write.mode(SaveMode.Overwrite).text(directorio + "mirflickr/labels")

    // Escribimos en HDFS para posteriores procesos la lista de imagenes y sus etiquetas (reducimos los 25000 ficheros de entrada a 6 ficheros)
    println("Escribimos imagenes-etiquetas de MIRFLICKR: " + Calendar.getInstance.getTime.toString )
    etiquetasDS.coalesce(6).write.mode(SaveMode.Overwrite).json(directorio + "mirflickr/labels-images")

    // Escribimos a disco todas las etiquetas con su cuenta para posterior análisis
    println("Escribimos Cuenta de todas las Etiquetas MIRFLICKR: " + Calendar.getInstance.getTime.toString )
    etqtasAgrMFDS.coalesce(6).write.mode(SaveMode.Overwrite).option("delimiter", "~").csv(directorio + "mirflickr/analisis")

    // Escribimos a disco las etiquetas originales
    println("Escribimos Cuenta de todas las Etiquetas MIRFLICKR: " + Calendar.getInstance.getTime.toString )
    etiquetasRDD.coalesce(6).toDF.write.mode(SaveMode.Overwrite).json(directorio + "mirflickr/original")

    // *****************************************
    // Categorización InceptionV3(TensorFlow)
    // *****************************************

    // Leemos puntuaciones de inception generadas por proceso lanzado previamente
    println("Leemos Etiquetas Inception: " + Calendar.getInstance.getTime.toString)
    val scoresInceptionDS = spark.read.json(directorio + "inception/clasification/part*").as[ScoresInception].cache()

    // Etiquetas Más comunes: Obtenemos las 50 más comunes, mostramos y escribimos a disco
    val etqtasCuentaTodasInception = scoresInceptionDS
      .filter("label <> '__None__'")
      .groupBy("label")
      .count.cache

    val etqtasMasComunesIDS = etqtasCuentaTodasInception
      .orderBy($"count".desc)
      .limit(50)
      .select("label")
      .orderBy($"label")

    println("Mostramos Etiquetas más comunes Inception: " + Calendar.getInstance.getTime.toString)
    etqtasMasComunesIDS.show()

    println("Escribimos en disco Etiquetas más comunes Inception: " + Calendar.getInstance.getTime.toString)
    etqtasMasComunesIDS.write.mode(SaveMode.Overwrite).text(directorio + "inception/comunes")

    // Todas las etiquetas (coalesce(6) para tener seis particiones y no doscientas)
    val etqtasTodasInception = etqtasCuentaTodasInception.select("label")
    println("Cuenta Etiquetas Inception: " + etqtasTodasInception.count + " :" + Calendar.getInstance.getTime.toString)
    println("Escribimos en disco Todas las Etiquetas Inception: " + Calendar.getInstance.getTime.toString)
    etqtasTodasInception.coalesce(6).write.mode(SaveMode.Overwrite).text(directorio + "inception/labels")

    // Escribimos las imágenes que tienen etqueta __None__ por error al procesar con TensorFlow
    val noneImagesInception = scoresInceptionDS.filter("label == '__None__'").select("image")
    println("Cuenta None Images Inception: " + noneImagesInception.count + " :" + Calendar.getInstance.getTime.toString)
    println("Escribimos en disco Todas las Imagenes con None de Inception: " + Calendar.getInstance.getTime.toString)
    noneImagesInception.coalesce(3).write.mode(SaveMode.Overwrite).text(directorio + "inception/none")

    // Todas las etiquetas para analisis(coalesce(6) para tener seis particiones y no doscientas)
    println("Escribimos en disco Todas las Etiquetas para analisis Inception: " + Calendar.getInstance.getTime.toString)
    etqtasCuentaTodasInception.coalesce(6).write.mode(SaveMode.Overwrite).option("delimiter", "~").csv(directorio + "inception/analisis")

    println("Fin Proceso: " + Calendar.getInstance.getTime.toString)
    spark.stop()

  }

}