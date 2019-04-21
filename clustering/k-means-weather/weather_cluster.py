import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits import mplot3d
from sklearn.cluster import KMeans
from sklearn import datasets
from mpl_toolkits.mplot3d import Axes3D

"""
W. Aldo Kusmik, WAK2116, ELEN-E6889, Spring 2019

Final Project
    This python file does offline clustering of weather data that was retrieved form 
    Google Pub/Sub and saved in CSV format
    
    The associated columns labels are localdatetime,icon,precip,summary,temp,time,uv,wind

"""
df = pd.read_csv('weather-data2.txt')
num_clusters = 6


#dates = np.array(df['localdatetime'])
#dates.sort()
#for r in range(0,len(dates)-1):
#    print(str(dates[r]))

x_train = df.drop('localdatetime', axis='columns')
x_train = x_train.drop('icon', axis='columns')
#x_train = x_train.drop('precip', axis='columns')
x_train = x_train.drop('summary', axis='columns')
#x_train = x_train.drop('temp', axis='columns')
x_train = x_train.drop('time', axis='columns')
#x_train = x_train.drop('uv', axis='columns')
#x_train = x_train.drop('wind', axis='columns')

#convert precipitation to a numeric value
#print(df.precip.unique())

def rain_num(x):
    return {
        'Clear': 0,
        'Partly Cloudy':0.1,
        'Mostly Cloudy':0.1,
        'Breezy and Mostly Cloudy': 0.1,
        'Overcast': 0.2,
        'Breezy and Overcast': 0.2,
        'Drizzle':0.4,
        'Light Rain': 0.6,
        'Rain': 0.8,
        'Heavy Rain': 1,
    }[x]

x_train['precip'] = x_train['precip'].apply(lambda x: rain_num(x))
#print(x_train.precip.unique())

#print(x_train)

# nomalize all attributes
precip_max = x_train['precip'].max()
x_train['precip'] = x_train['precip'].apply(lambda x: x/precip_max)
temp_max= x_train['temp'].max()
x_train['temp'] = x_train['temp'].apply(lambda x: x/temp_max)
uv_max= x_train['uv'].max()
x_train['uv'] = x_train['uv'].apply(lambda x: x/uv_max)
wind_max = x_train['wind'].max()
x_train['wind'] = x_train['wind'].apply(lambda x: x/wind_max)

#print(x_train)


np.random.seed(5)

cluster = KMeans(n_clusters=num_clusters).fit(x_train)
prediction = cluster.predict(x_train)
#print(prediction)

fig = plt.figure()
ax = plt.axes(projection='3d')

xdata = x_train['temp']
ydata = x_train['wind']
zdata = x_train['precip']

cluster_number = np.arange(len(x_train))
cluster_number = x_train['wind'].apply(lambda x: 1)
cdict = {0: 'blue', 1: 'green', 2: 'red', 3:'cyan',
         4: 'magenta', 5: 'yellow', 6: '0.1', 7: '0.2',
         8: '0.4', 9: '0.6', 10: '0.8', 11: '0.1', 12: 'black'}\

# print cluster centers

centers = np.array(cluster.cluster_centers_)

#print(centers)

for ind in range(0,len(centers)):
    output_str = str(ind)
    output_str += ": precip = " + "{:.1f}".format(centers[ind,0] * precip_max)
    output_str += ", temp = " + "{:.1f}".format(centers[ind,1] * temp_max)
    output_str += ", uv = " + "{:.1f}".format(centers[ind,2] * uv_max)
    output_str += ", wind = " + "{:.1f}".format(centers[ind,3] * wind_max)
    ax.scatter3D(centers[ind,1], centers[ind,3], centers[ind,0], c=cdict[ind])
    print(output_str)


# plot clusters
#for index in range(0,len(prediction)):
    #print(prediction[index])
    #ax.scatter3D(xdata[index], ydata[index], zdata[index], c=cdict[prediction[index]])

#ax.scatter3D(xdata,ydata,zdata,c=zdata,cmap='Blues')

ax.set_title("NYC Weather Data")
ax.set_xlabel('Temperature')
ax.set_ylabel('Wind')
ax.set_zlabel('Precipitation')
plt.xlim(0,1)
plt.ylim(0,1)
ax.set_zlim(0,1)
plt.savefig("cluster" + str(num_clusters) + ".png")
plt.show()







