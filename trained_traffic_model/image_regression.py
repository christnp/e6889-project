
import numpy as np
import pandas as pd
from sklearn import model_selection
import tensorflow as tf
from pathlib import Path

"""
    W. Aldo Kusmik, WAK2116, ELEN-E6889, Spring 2019

    Final Project
    This python file trains a neural network that predicts an activity level
    based on a jpg image from a traffic camera

    This is an initial attempt at doing regression based on image data.
    It is loosely based on TF image classification examples and 
    "Deep Leaning: Image Recognition" on Lynda.com 
"""

# view sample image
img_path = "./labeled_data/"

df = pd.read_csv('./labeled_data/labels.txt')
#print(df)

df_train, df_valid = model_selection.train_test_split(df, test_size=.1)

#print(df_train)
#print("---")
#print(df_valid)

def keras_data(data):

    # Output arrays
    x = np.empty([0, 160, 160, 3], dtype=np.float32)
    y = np.empty([data.datetime.count()], dtype=np.float32)
    #print(x.shape)
    #print(y.shape)

    # Read in and preprocess a batch of images
    sess = tf.Session()

    for i in range(0, data.datetime.count()):
        #print(data.people[data.index[i]])
        y_value = data.vehicles[data.index[i]] + data.people[data.index[i]]
        #print(y_value)
        #y = np.append(y, [y_value])
        y[i] = y_value

        # convert image to a tensor
        # img_raw = tf.read_file(sample_img_path)
        image_file = img_path + data.datetime[data.index[i]]
        img_raw = tf.read_file(image_file)

        #print(repr(img_raw)[:200]+"...")
        img_tensor = tf.image.decode_image(img_raw)
        #print(img_tensor.shape)
        cropped_tensor = tf.image.crop_to_bounding_box(img_tensor,80, 80, 160, 220)
        #print(cropped_tensor.shape)

        #output_image = tf.image.encode_png(cropped_tensor)
        #file = tf.write_file("text.png",output_image)

        img_tensor = tf.image.resize(cropped_tensor, [160, 160])
        #img_tensor = tf.image.resize(img_tensor, [240, 240])  # squish it down a bit
        img_tensor /= 255.0  # normalize to [0,1] range
        # print(img_tensor)
        #print(img_tensor.shape)
        # print(img_tensor.dtype)

        sess = tf.Session()
        with sess.as_default():
            np_array = img_tensor.eval()
            #print("np from tensor", np_array.shape)
            indexed_array = np.expand_dims(np_array, axis=0)
            #print("np from tensor with index",indexed_array.shape)
            x = np.append(x, indexed_array, axis=0)
            #print("x shape", x.shape)

    #print(y.shape)
    return x, y

x_train, y_train = keras_data(df_train)
x_test, y_test = keras_data(df_valid)

#y_train = tf.keras.utils.to_categorical(y_train, 16)
#y_test = tf.keras.utils.to_categorical(y_test, 16)
y_train = y_train / 16
y_test = y_test / 16


#(x_train, y_train), (x_test,y_test) = tf.keras.datasets.cifar10.load_data()
#x_train = x_train.astype("float32")
#x_test = x_test.astype("float32")
#x_train = x_train / 255
#x_test = x_test / 255

#y_train = tf.keras.utils.to_categorical(y_train, 10)
#y_test = tf.keras.utils.to_categorical(y_test, 10)

model = tf.keras.Sequential()
#model.add(tf.keras.layers.Conv2D(32,(3,3), padding='same', activation='relu', input_shape=(32, 32, 3)))
model.add(tf.keras.layers.Conv2D(32,(3,3), padding='same', activation='relu', input_shape=(160, 160, 3)))

model.add(tf.keras.layers.Conv2D(32,(3,3), activation='relu'))
model.add(tf.keras.layers.MaxPooling2D(2,2))
model.add(tf.keras.layers.Dropout(0.25))

model.add(tf.keras.layers.Conv2D(64,(3,3), padding='same', activation='relu'))
model.add(tf.keras.layers.Conv2D(64,(3,3), activation='relu'))
model.add(tf.keras.layers.MaxPooling2D(2,2))
model.add(tf.keras.layers.Dropout(0.25))

model.add(tf.keras.layers.Flatten())

model.add(tf.keras.layers.Dense(512, activation="relu"))
model.add(tf.keras.layers.Dropout(0.5))

model.add(tf.keras.layers.Dense(100, activation='relu'))
model.add(tf.keras.layers.Dropout(.25))

#model.add(tf.keras.layers.Dense(10, activation="softmax"))
model.add(tf.keras.layers.Dense(10, activation="relu"))
model.add(tf.keras.layers.Dense(1))


model.compile(
    #loss='categorical_crossentropy',
    loss='mse',
    optimizer='adam',
    metrics=["accuracy", "mae"]
)

model.summary()


model.fit(
    x_train,
    y_train,
    batch_size=10,
    epochs=30,
    validation_data=[x_test, y_test],
    shuffle=True  #,
    #steps_per_epoch=1000
)

# save structure
model_structure = model.to_json()
f = Path("model_structure.json")
f.write_text(model_structure)
# save weights
model.save_weights("model_weights.h5")


