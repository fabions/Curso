from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("FirstSparkApp")
sc = SparkContext(conf = conf)

rdd = sc.textFile("file:///d:/Cursos/Spark/Aula00-Environment/applications/u.data")
print(rdd.first())

ratings = rdd.map(lambda line : line.split()[2])
result = ratings.countByValue()
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))