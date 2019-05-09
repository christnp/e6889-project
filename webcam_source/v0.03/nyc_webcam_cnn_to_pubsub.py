import time
import datetime
import random
import requests
import tensorflow as tf
from google.cloud import pubsub
from pathlib import Path
import numpy as np
from PIL import Image
from io import BytesIO

"""
    W. Aldo Kusmik, WAK2116, ELEN-E6889, Spring 2019
    Final Project
    This python file connects to the NYC traffic camera system website, retrieves jpg traffic camera images, and
    runs that image through a convolutional neural network to generate a float between 0 and 1. 
    (0 represents little activity and 1 represents considerable activity)
    
    The resulting value is forwarded to Google Cloud Pub/Sub as an unbounded source for the ELEN-E6889 final project.
    There are three cameras that are in close proximity to the Columbia University COSMOS testbed
    - cctv1005 is located at Broadway & West 125th
    - cctv689 is located at Amsterdam & West 125th
    - cctv688 is located at St Nicholas Ave & West 125th
    This python version only contains a small subset of the functionality from the alternate java version
    
    Version: nyc_webcam_to_pubsub v0.03
"""

#tf.enable_eager_execution()

# configure Google Cloud PubSub
topic = "projects/elene6889/topics/traffic-topic"
publisher = pubsub.PublisherClient()

camera_id = "689"  # nyc camera number

# load CNN
f = Path("CNN/model_structure.json")
model_structure = f.read_text()
model = tf.keras.models.model_from_json(model_structure)
model.load_weights("CNN/model_weights.h5")


while True:

    # get current time
    gmt_datetime = time.strftime('%Y-%m-%d-%H-%M-%S', time.gmtime())
    dt_index = datetime.datetime.strptime(gmt_datetime, '%Y-%m-%d-%H-%M-%S')
    # convert GMT to NYC time
    dt_index -= datetime.timedelta(seconds=(60 * 60 * 4))
    nyc_datetime = dt_index.strftime('%Y-%m-%d-%H-%M-%S')

    # fetch image
    #sample_img_path = "/Users/wak/PycharmProjects/ELEN6889_TensorFlow/689-2019-04-04-08-34-38.jpg"

    nyc_URL = "http://207.251.86.238/cctv" + camera_id + ".jpg?math=" + str(random.uniform(0, 1));
    #print(nyc_URL)
    r = requests.get(nyc_URL)
    if (r.status_code == 200):
        #i = Image.open(BytesIO(r.content))
        #i.show()
        #print("Request successful")

        # push file to disk
        filename = "images/" + camera_id + "-" + nyc_datetime + ".jpg"
        newFileByteArray = bytearray(r.content)
        file = open(filename, "wb")
        file.write(newFileByteArray)
        file.close()

        # convert image to a value
        img = tf.keras.preprocessing.image.load_img(filename)
        cropped_tensor = tf.image.crop_to_bounding_box(img, 80, 80, 160, 220)
        img_tensor = tf.image.resize(cropped_tensor, [160, 160])
        img_tensor /= 255.0  # normalize to [0,1] range

        list_of_images = np.expand_dims(img_tensor, axis=0)

        sess = tf.Session()
        with sess.as_default():
            np_array = img_tensor.eval()
            # print(np_array)
            indexed_array = np.expand_dims(np_array, axis=0)
            # print(indexed_array.shape)

        results = model.predict(indexed_array)
        predict = str(results[0][0])
        #print(results,predict)

        pubsub_data = str(results)


        # push tensor to Google PubSub
        publisher.publish(topic, pubsub_data.encode('utf-8'), camera=camera_id, localdatetime=nyc_datetime,
                          tensor_shape=str(img_tensor.shape), tensor_datatype=str(img_tensor.dtype),
                          cnn_output=predict)

        print("traffic_image:" + camera_id + " (" + nyc_datetime + ')')

    else:
        print("No image. Status code:" + r.status_code)

    time.sleep(60)


