spark-submit \
  --master spark://master.spark.tfm:7077 \
  --conf spark.executor.memory=2g \
  /home/utad/TFM/Fuentes/TensorFlowMirFlickr/ClasificacionImagenes.py

spark-submit \
  --class AnalisisEtiquetas \
  --conf spark.executor.memory=2g \
  --master spark://master.spark.tfm:7077 \
  /home/utad/TFM/Fuentes/SparkInferenciaEtiquetas/target/scala-2.11/sparkinferenciaetiquetas_2.11-1.0.jar

spark-submit \
  --class InfFlickrtoInception \
  --conf spark.executor.memory=2g \
  --master spark://master.spark.tfm:7077 \
  /home/utad/TFM/Fuentes/SparkInferenciaEtiquetas/target/scala-2.11/sparkinferenciaetiquetas_2.11-1.0.jar 

spark-submit \
  --class InfInceptiontoFlickr \
  --conf spark.executor.memory=2g \
  --master spark://master.spark.tfm:7077 \
  /home/utad/TFM/Fuentes/SparkInferenciaEtiquetas/target/scala-2.11/sparkinferenciaetiquetas_2.11-1.0.jar
