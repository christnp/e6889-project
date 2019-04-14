import time
import datetime
import random
import requests
import tensorflow as tf
from google.cloud import pubsub
from PIL import Image
from io import BytesIO

"""
    W. Aldo Kusmik, WAK2116, ELEN-E6889, Spring 2019

    Final Project
    This python file connects to the NYC traffic camera system website, retrieves jpg traffic camera images, and
    forwards a tensor to Google Cloud Pub/Sub to serve as a candidate source for the ELEN-E6889 final project.

    There are three cameras that are in close proximity to the Columbia University COSMOS testbed
    - cctv1005 is located at Broadway & West 125th
    - cctv689 is located at Amsterdam & West 125th
    - cctv688 is located at St Nicholas Ave & West 125th

    This python version only contains a small subset of the functionality from the alternate java version

    Version: nyc_webcam_to_pubsub v0.02

"""

tf.enable_eager_execution()

# configure Google Cloud PubSub
topic = "projects/elene6889/topics/traffic-topic"
publisher = pubsub.PublisherClient()

camera_id = "689"  # nyc camera number

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

        # convert image to a tensor
        #img_raw = tf.read_file(sample_img_path)
        img_raw = tf.read_file(filename)
        #pubsub_data = str(img_raw)
        #print(repr(img_raw)[:100]+"...")
        img_tensor = tf.image.decode_image(img_raw)
        img_tensor = tf.image.resize(img_tensor, [240,240]) # squish it down a bit
        img_tensor /= 255.0 # normalize to [0,1] range
        #print(img_tensor)
        #print(img_tensor.shape)
        #print(img_tensor.dtype)

        pubsub_data = str(img_tensor)

        # push tensor to Google PubSub
        publisher.publish(topic, pubsub_data.encode('utf-8'), camera=camera_id, localdatetime=nyc_datetime,
                          tensor_shape=str(img_tensor.shape), tensor_datatype=str(img_tensor.dtype))

        print("traffic_image:" + camera_id + " (" + nyc_datetime + ')')

    else:
        print("No image. Status code:" + r.status_code)

    time.sleep(60)



