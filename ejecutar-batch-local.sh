spark-submit \
  --master local[3] \
  --conf spark.executor.memory=4g \
  /home/utad/TFM/Fuentes/TensorFlowMirFlickr/ClasificacionImagenes.py \
  -d file:/mnt/hgfs/TFM/ficheros/inception/classification

spark-submit \
  --class AnalisisEtiquetas \
  --conf spark.executor.memory=4g \
  --master local[3] \
  /home/utad/TFM/Fuentes/SparkInferenciaEtiquetas/target/scala-2.11/sparkinferenciaetiquetas_2.11-1.0.jar \
  file:/mnt/hgfs/TFM/ficheros/

spark-submit \
  --class InfFlickrtoInception \
  --conf spark.executor.memory=4g \
  --master local[3] \
  /home/utad/TFM/Fuentes/SparkInferenciaEtiquetas/target/scala-2.11/sparkinferenciaetiquetas_2.11-1.0.jar \
  file:/mnt/hgfs/TFM/ficheros/

spark-submit \
  --class InfInceptiontoFlickr \
  --conf spark.executor.memory=4g \
  --master local[3] \
  /home/utad/TFM/Fuentes/SparkInferenciaEtiquetas/target/scala-2.11/sparkinferenciaetiquetas_2.11-1.0.jar \
  file:/mnt/hgfs/TFM/ficheros/
