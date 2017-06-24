#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Módulos a usar
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark import SparkContext
import os.path
import urllib
from urllib2 import HTTPError
import tarfile
import tensorflow as tf
import numpy as np
import Constantes as C
from NodeLookup import NodeLookup
from hdfs import Config
from time import time
from datetime import datetime


# Obtenemos modelo
def get_tensorflow_model():
    # Download and extract model tar file
    filename = C.MODEL_URL.split('/')[-1]
    filepath = os.path.join(C.model_dir, filename)
    if not os.path.exists(filepath):
        filepath2, _ = urllib.urlretrieve(C.MODEL_URL, filepath)
        print("filepath2", filepath2)
        statinfo = os.stat(filepath)
        print('Succesfully downloaded', filename, statinfo.st_size, 'bytes.')
        tarfile.open(filepath, 'r:gz').extractall(C.model_dir)
    else:
        print('Model already downloaded:', filepath, os.stat(filepath))


def run_inference_on_image(sess, image, lookup):
    """Hacemos inferencia sobre la imagen.

    Args:
        sess: TensorFlow Session
        image: Imagen a leer
        lookup: node lookup obtenido previamente

    Returns:
      (image ID, image URL, scores),
      where scores is a list of (human-readable node names, score) pairs
    """
    try:
        image_data = urllib.urlopen(image).read()
    except HTTPError as eH:
        print(eH.code, eH.read())
        return image, [['__None__', 1.0]]
    # Some useful tensors:
    # 'softmax:0': A tensor containing the normalized prediction across
    #   1000 labels.
    # 'pool_3:0': A tensor containing the next-to-last layer containing 2048
    #   float description of the image.
    # 'DecodeJpeg/contents:0': A tensor containing a string providing JPEG
    #   encoding of the image.
    # Runs the softmax tensor by feeding the image_data as input to the graph.
    softmax_tensor = sess.graph.get_tensor_by_name('softmax:0')
    try:
        predictions = sess.run(softmax_tensor, {'DecodeJpeg/contents:0': image_data})
    except:
        # Handle problems with malformed JPEG files
        return image, [['__None__', 1.0]]
    predictions = np.squeeze(predictions)
    top_k = predictions.argsort()[-C.max_etiquetas:][::-1]
    scores = []
    for node_id in top_k:
        if node_id not in lookup:
            human_string = ''
        else:
            human_string = lookup[node_id]
        score = predictions[node_id]
        scores.append((human_string, score))
    return image, scores


def procesar_lote_imagenes(lote):
    """Hace la inferencia sobre un lote de imágenes.
    Obtiene categorias inferidas de cada imagen del lote
    Serializa cada imagen del lote
    Aplana la lista de listas obtenidas

    We do not explicitly tell TensorFlow to use a GPU.
    It is able to choose between CPU and GPU based on its guess of which will be faster.

    :param lote: [imagenes]
    :return: [Row(image, label, score)]
    """
    with tf.Graph().as_default() as g:
        graph_def = tf.GraphDef()
        graph_def.ParseFromString(model_data_bc.value)
        _ = tf.import_graph_def(graph_def, name='')
        with tf.Session() as sess:
            labeled = [run_inference_on_image(sess, image, node_lookup_bc.value) for image in lote]
            lista_serializada = [serializar_inferencia(scores) for scores in labeled]
            return [t for inferencia in lista_serializada for t in inferencia]


def serializar_inferencia(tupla):
    """Dada una tupla (ruta imagen, [(etiqueta, scores)]) devuelve una serie de Rows [(ruta imagen, etiqueta, score)]
    se utiliza Row ya que se quiere utilizar data frame para grabar como Json

    :param tupla: (ruta imagen, [(etiqueta, scores)])
    :return: [Row(image, label, score)]
    """
    # Nos quedamos solo con el nombre de la imagen
    imagen = tupla[0].split("/")[-1].split(".")[0]
    return [Row(image=imagen, label=t[0], score=float(t[1])) for t in tupla[1]]


# Función auxiliar para obtener el nombre de una imagen.
def obtener_nombre_imagen(x):
    return C.IMAGES_INDEX_URL + x.split('<')[1].split('>')[1]

# ***************************************************************************
# Inicio del proceso
# ***************************************************************************

# Iniciamos SparkContext
print("Inicio: ", datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H:%M:%S'))
sc = SparkContext('spark://master.spark.tfm:7077', 'TensorFlow',
                  pyFiles=['/home/utad/TFM/Fuentes/TensorFlowMirFlickr/Constantes.py',
                           '/home/utad/TFM/Fuentes/TensorFlowMirFlickr/NodeLookup.py'])
# sc = SparkContext('local')
get_tensorflow_model()

# Cargamos el modelo y lo distribuimos
model_path = os.path.join(C.model_dir, 'classify_image_graph_def.pb')
with tf.gfile.FastGFile(model_path, 'rb') as f:
    model_data = f.read()
model_data_bc = sc.broadcast(model_data)

# Distribuimos node lookup para ser utilizado en los workers
node_lookup = NodeLookup().node_lookup
node_lookup_bc = sc.broadcast(node_lookup)

# Obtenemos una lista de las imágenes a procesar y las agrupamos en lotes
servicio_imagenes = None
try:
    servicio_imagenes = urllib.urlopen(C.IMAGES_INDEX_URL)
except Exception as e:
    print(e)
    print("Servidor de imágenes no disponible")
    exit(404)

imagenes = servicio_imagenes.read().split('<li>')[2:C.numero_imagenes_proceso + 2]
lote_imagenes = [imagenes[i:i + C.lote_size] for i in range(0, len(imagenes), C.lote_size)]

# Paralelizamos los lotes de imagenes y procesamos
rdd_imagenes = sc.parallelize(lote_imagenes).map(lambda x: map(obtener_nombre_imagen, x))
inception_rdd = rdd_imagenes.flatMap(procesar_lote_imagenes)

# Borramos directorio categorias del hdfs por si existiera
client = Config().get_client()
client.delete('inception', recursive=True)

# Salvamos los ficheros obtenidos en formato json. Para ello hay que usar un dataframe
print("Procesamos:", datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H:%M:%S'))
spark = SparkSession(sc)
inception_df = inception_rdd.toDF()
print("Salvamos:", datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H:%M:%S'))
inception_df.write.json('hdfs://master.spark.tfm:9000/user/utad/inception/classification')
print("Fin:", datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H:%M:%S'))
