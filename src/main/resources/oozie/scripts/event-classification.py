# -----
# This is for clustering events into about 35 categories
#
# Please run the following code in Jupyter Notebook with PySpark

# ------
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ------
spark = SparkSession.builder.getOrCreate()

# ------
df = spark.read.format("csv").option("header", "true").load("file:///root/events.csv") \
  .drop("user_id") \
  .drop("start_time") \
  .drop("city") \
  .drop("state") \
  .drop("zip") \
  .drop("country") \
  .drop("lat") \
  .drop("lng")
  
# ------  
dfEvents = df.select([col(c).cast(DoubleType()) for c in df.columns])

# ------  
feature_columns = ["c_" + str(i) for i in range(1, 101)]
feature_columns.append('c_other')

# ------
vecAssembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
dfTrain = vecAssembler.transform(dfEvents).select('event_id', 'features')

# ------
import numpy as np
import pandas as pd

cost = np.zeros(37)
for k in range(2, 37):
    kmeans = KMeans().setK(k).setSeed(7).setFeaturesCol("features")
    model = kmeans.fit(dfTrain.sample(False, 0.1, seed = 37))
    cost[k] = model.computeCost(dfTrain)
	
# ------
import matplotlib.pyplot as plt
%matplotlib inline

fig, ax = plt.subplots(1, 1, figsize = (8, 6))
ax.plot(range(2, 37), cost[2:37])
ax.set_xlabel('k')
ax.set_ylabel('cost')

# ------
k = 35
kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
model = kmeans.fit(dfTrain)
centers = model.clusterCenters()

print("Cluster Centers: ")
for center in centers:
    print(center)
	
# ------	
ransformed = model.transform(dfTrain).select('event_id', 'prediction')
rows = transformed.collect()
print(rows[:3])