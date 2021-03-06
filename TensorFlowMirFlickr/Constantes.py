#!/usr/bin/env python
# -*- coding: utf-8 -*-

# **************************************************************************************
# Configuración proceso
# Modelo lo bajamos de la red y lo guardamos en directorio local
MODEL_URL = 'http://download.tensorflow.org/models/image/imagenet/inception-2015-12-05.tgz'
model_dir = '/home/utad/TFM/model'

# Simulamos que las imagenes las obtenermos de un servicio web
images_index_url = 'http://host.images.tfm:8000/mirflickr/'

# Directorio de salida
dir_classification = 'hdfs://master.spark.tfm:9000/user/utad/inception/classification'

# Otros datos
numero_imagenes_proceso = 25000  # Número total de imágenes a procesar
lote_size = 200  # Número de imágenes por lote
max_etiquetas = 5  # Número máximo de etiquetas por imagen

# Fin Configuración proceso
# **************************************************************************************
