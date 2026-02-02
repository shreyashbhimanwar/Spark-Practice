from pyspark import SparkConf, SparkContext
import collections
import time

start_time = time.time()

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

# Divdes each line of u.data into one value in RDD
lines = sc.textFile("Datasets/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2]) # Transformation
result = ratings.countByValue() # Action

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))

end_time = time.time()
print("Execution time: %.2f seconds" % (end_time - start_time))