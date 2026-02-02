from pyspark import SparkConf, SparkContext
import time

conf = SparkConf().setMaster("local").setAppName("FakeFriends")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

data = sc.textFile("Datasets/fakefriends.csv")
rdd = data.map(parseLine)

new_rdd = rdd.mapValues(lambda x: (x, 1))
new_rdd = new_rdd.reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1]))
print(new_rdd.take(5))
new_rdd = new_rdd.mapValues(lambda x: x[0] / x[1])
# This above section didnt work because mapValues is a Transformation,
# no Action has been called to execute the DAG.
print(new_rdd.take(5)) # take() is an Action that triggers the execution of the DAG.

result = new_rdd.sortByKey().collect()
for key, value in result:
    print("%s %f" % (key, value))
