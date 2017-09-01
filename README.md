# Spark: Visión artificial con TensorFlow e inferencia entre categorías

**Trabajo final Programa Experto Big Data - U-tad**

**Roberto Méndez Julio 2017**

## Documentación del proyecto
Puede encontrar más información del proyecto en: <https://goo.gl/DDuQq7>

El código fuente del proyecto se encuentra aquí: <https://github.com/robzoros/TFP-Utad-InferenciaEtiquetas>

Puede verse una demo de la web en:  <https://zoroastro.neocities.org/> y <https://zoroastro.neocities.org/storm.html>

Video en: <https://youtu.be/pPR_FIH7Lm8>

## Arquitectura
### Infraestructura utilizada:
- Ordenador 16Gb memoria y 4 nucleos
- Máquinas virtuales ubuntu 16.04 sobre VMware
- 1 Master / Namenode: 4gb y 1 núcleo
- 3 Workers  / Datanodes: 3gb y 1 núcleo
- Spark 2.1.1 en modo Standalone
- Hadoop 2.7.3
- Scala 2.11.8
- Python 2.7

## Clasificación de Imágenes
Clasificación de imágenes aplicando el modelo entrenado de reconocimiento de imágenes Inception-V3 con TensorFlow. Uso de Spark para paralelización y escalabilidad y volcado de resultados en fichero json en HDFS.
Se obtiene por cada imagen varios registros con el formato siguiente (**image** es el identificador de la imagen, un número entre 1 y 25000, **label** etiqueta de ImageNet que el algoritmo ha determinado como probable, **score** puntuación que el algoritmo establece indicando probabilidad de que la etiqueta se corresponda realmente con la imagen )
    {"image": String, "label": String, "score": Double}

## Análisis de metadatos:
Para cada imagen del set Mirflickr tiene un fichero con las etiquetas que el usuario puso en Flickr cuando subió la foto

El proceso realiza un análisis de las etiquetas de Mirflickr para sacar información relevante de las mismas y para intentar agrupar las etiquetas que tienen la misma información. Para ello realiza las siguientes operaciones sobre las etiquetas:
- Tokenización de etiquetas (frases convertidas en palabras)
- Normalización de acentos
- Normalización de metadatos fotográficos
- Normalización de datos geolocalizados
- Trabajar sólo con palabras singulares 
- Uso de StopWords para eliminar palabras no significativas

### Salida del proceso:
#### Inception:
- fichero analisis: csv con tuplas (etiqueta, número de ocurrencias)
- fichero comunes: txt con las 50 etiquetas más comunes
- fichero labels: txt con todas las etiquetas
- fichero none: txt con imágenes sin etiqueta

#### Mirflickr
- fichero analisis: csv con tuplas (etiqueta, número de ocurrencias)
- fichero comunes: txt con las 50 etiquetas más comunes
- fichero idiomas: csv con tuplas (idioma, número de ocurrencias)
- fichero labels: txt con todas las etiquetas
- fichero labels-images: json con tuplas (imagen, etiqueta procesada)
- fichero original: json con tuplas (imagen, etiqueta original)

## Entrenamiento Modelo inferencia entre categorías
Entrenamiento de un modelo con las etiquetas que ponen los usuarios a las fotos que suben a Flickr para inferir si el modelo Inception-v3 entrenado con TensorFlow va a clasificar una imagen con una determinada etiqueta.

Igualmente se realiza un entrenamiento de un modelo con las etiquetas obtenidas al aplicar el modelo Inception-v3, para inferrir si un usuario de Flickr pondrá una determinada etiqueta.

Se entrenan 50 modelos (uno por etiqueta) para las 50 etiquetas más comunes puestas por los usuarios y 50 modelos (uno por etiqueta) para las 50 etiquetas más comunes obtenidas con Inception.

## Visualización de datos
Página web programada con D3js para ver resultado de proceso de inferencia entre categorías de manera gáfica así como otros datos de interés como por ejemplo datos sobre imágenes procesadas o uso de idioma.

## Clasificación imágenes de Twitter
Como extra se ha realizado un proceso para clasificar imágenes en tiempo real usando Storm.

Arquitectura sobre misma la infraestructura:
- Zookeeper en máquina virtual de 4gb y un núcleo
- Nimbus en misma máquina virtual que Zookeeper
- Supervisores en las otras 3 máquinas virtuales con 3gb y un núcleo (4 workers por supervisor).

## Links
### General:
- Documentación Apache Spark 2.1.1: <https://spark.apache.org/docs/2.1.1/index.html>
- Documentación apache Hadoop 2.7.3: <http://hadoop.apache.org/docs/r2.7.3/>

### Clasificación de Imágenes:
- Mirflickr: <http://press.liacs.nl/mirflickr/>
- TensorFlow reconocimiento de imágenes: <https://www.tensorflow.org/tutorials/image_recognition>
- Imagenet - Etiquetas usadas para clasificar por algoritmo Inception-v3: <http://image-net.org/>

### Análisis de Datos:
- LangId para detectar Idioma: <https://github.com/saffsd/langid.py>
- Scala Inflector para obtener singular: <https://github.com/backchatio/scala-inflector>

### Entrenamiento modelo y predicciones:
- Spark MLlib guide: <https://spark.apache.org/docs/2.1.1/ml-guide.html>

### Visualización de datos:
- Documentación D3: <https://github.com/d3/d3-3.x-api-reference/blob/master/API-Reference.md>

### Clasificación imágenes Twitter:
- Storm: <http://storm.apache.org/releases/1.1.0/index.html>
- Documentación Streaming Api Twitter: <https://dev.twitter.com/streaming/overview>
