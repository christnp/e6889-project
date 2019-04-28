from pathlib import Path
import pandas as pd
from os import listdir
import tensorflow as tf
import numpy as np

"""
W. Aldo Kusmik, WAK2116, ELEN-E6889, Spring 2019

Final Project
    This file is used run trained model


"""

file_list = listdir("../globus/images/")
print(len(file_list))

f = Path("model_structure.json")
model_structure = f.read_text()
model = tf.keras.models.model_from_json(model_structure)
model.load_weights("model_weights.h5")

df = pd.DataFrame(file_list, columns=["localdatetime"])
df = df.sort_values(by=["localdatetime"])
df = df.reset_index(drop=True)
df["predict"] = pd.Series(-0.1, index=df.index)

#print(df)

first = 18000
last = 18488
print("processing",first,last)

df1 = df.iloc[first:last]

#for i in range(0,len(file_list)):
for i in range(first,last):

    input_img = df.localdatetime[i]
    timestamp = input_img[4:20]
    #print(timestamp)

    img = tf.keras.preprocessing.image.load_img("../globus/images/"+ input_img)
    cropped_tensor = tf.image.crop_to_bounding_box(img,80, 80, 160, 220)
    img_tensor = tf.image.resize(cropped_tensor, [160, 160])
    img_tensor /= 255.0  # normalize to [0,1] range

    #print(img_tensor.shape)
    #image_to_test = tf.keras.preprocessing.image.img_to_array(img)

    list_of_images = np.expand_dims(img_tensor, axis=0)

    sess = tf.Session()
    with sess.as_default():
        np_array = img_tensor.eval()
        #print(np_array)
        indexed_array = np.expand_dims(np_array, axis=0)
        #print(indexed_array.shape)

    results = model.predict(indexed_array)
    df1.localdatetime.iat[i-first] = timestamp
    df1.predict.iat[i-first] = results
    #print(results)

#print(df)
df1.to_csv("traffic_model_outputs-"+ str(first) + ".txt",index=False)


