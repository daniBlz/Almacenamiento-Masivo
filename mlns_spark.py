from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import time
conf = SparkConf().setAppName("MLNS").setMaster("local[*]")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.appName("MLNS").getOrCreate()
movies = spark.read.csv('./movies.csv', header=True, inferSchema = 'true')
ratings = spark.read.csv('./ratings.csv',header = True, inferSchema = 'true')  
movies.show(5)
ratings.show(5)
#Ratings rating value count ordenados de mayor a menor
start_time = time.time()
ratings.groupBy('rating').count().orderBy('count', ascending=False).show()
print("--- %s seconds ---" % (time.time() - start_time))
start_time = time.time()
means = ratings.groupBy('movieID').agg({'rating':'mean'})
movie_rating = movies.join(means, on = 'movieID', how = 'inner')
movie_rating.show(5)
print("--- %s seconds ---" % (time.time() - start_time))
movie_rating.write.csv('movie_rating.csv')
